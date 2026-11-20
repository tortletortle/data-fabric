// HyperFlow V0.9: cross-shard aggregation, Linux C++20.
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <thread>
#include <vector>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
using namespace std; using Clock=chrono::steady_clock;
static uint64_t ns(){return chrono::duration_cast<chrono::nanoseconds>(Clock::now().time_since_epoch()).count();}
static void check(bool x,const char* m){if(!x)throw runtime_error(m);}
struct FD{int fd;~FD(){if(fd>=0)close(fd);}};
struct Frame{array<uint32_t,2> h;const vector<unsigned char>* p;size_t size()const{return 8+p->size();}};
struct Q{mutex m;condition_variable cv;vector<Frame> slots;size_t cap,head=0,count=0,used=0,peak=0;uint64_t lock_ns=0,waits=0;explicit Q(size_t c):slots(c/8),cap(c){}};
class Sender{
    int fd;vector<unique_ptr<Q>> qs;size_t batch;bool aggregate;atomic<uint64_t> epoch{0};atomic<bool> closing{false},failed{false};thread worker;
    void signal(){epoch.fetch_add(1,memory_order_release);epoch.notify_one();}
    void close_all(){closing.store(true);for(auto& q:qs){lock_guard l(q->m);q->cv.notify_all();}signal();}
    void run(){size_t cursor=0;array<iovec,64> v{};
        struct Reservation{size_t shard,n,bytes;};vector<Reservation> reserved;reserved.reserve(qs.size());
        for(;;){auto e=epoch.load(memory_order_acquire);size_t total=0,k=0;reserved.clear();
            for(size_t j=0;j<qs.size()&&total<batch;++j){size_t i=(cursor+j)%qs.size();auto& q=*qs[i];lock_guard l(q.m);if(!q.count)continue;size_t n=min(batch-total,q.count),bytes=0;
                for(size_t t=0;t<n;++t){auto& f=q.slots[(q.head+t)%q.slots.size()];v[k++]={f.h.data(),8};if(!f.p->empty())v[k++]={const_cast<unsigned char*>(f.p->data()),f.p->size()};bytes+=f.size();}reserved.push_back({i,n,bytes});total+=n;if(!aggregate)break;}
            if(reserved.empty()){if(closing.load()){if(epoch.load(memory_order_acquire)!=e)continue;break;}epoch.wait(e,memory_order_acquire);continue;}
            ++batches;if(reserved.size()>1)++mixed_batches;
            for(size_t i=0;i<k;){msghdr m{};m.msg_iov=v.data()+i;m.msg_iovlen=k-i;ssize_t r=sendmsg(fd,&m,MSG_NOSIGNAL);++calls;
                if(r<0&&errno==EINTR)continue;
                if(r<=0){failed=true;shutdown(fd,SHUT_RDWR);close_all();return;}
                size_t a=r;while(i<k&&a>=v[i].iov_len){a-=v[i++].iov_len;}if(a){v[i].iov_base=static_cast<char*>(v[i].iov_base)+a;v[i].iov_len-=a;}}
            for(auto r:reserved){auto& q=*qs[r.shard];lock_guard l(q.m);q.head=(q.head+r.n)%q.slots.size();q.count-=r.n;q.used-=r.bytes;q.cv.notify_all();}cursor=(reserved.back().shard+1)%qs.size();
        }shutdown(fd,SHUT_WR);
    }
public:
    uint64_t calls=0,batches=0,mixed_batches=0;
    Sender(int f,size_t cap,size_t shards,size_t b=32,bool a=false):fd(f),batch(b),aggregate(a){check(shards&&cap%shards==0&&cap/shards>=8&&b&&b<=32,"bad config");for(size_t i=0;i<shards;i++)qs.push_back(make_unique<Q>(cap/shards));timeval t{3,0};check(!setsockopt(fd,SOL_SOCKET,SO_SNDTIMEO,&t,sizeof(t)),"timeout");worker=thread([this]{run();});}
    ~Sender(){close_all();if(worker.joinable())worker.join();}
    // Payload must remain immutable and alive through finish().
    void submit(size_t producer,uint32_t id,const vector<unsigned char>& p){auto& q=*qs[producer%qs.size()];check(p.size()<=UINT32_MAX&&p.size()<=q.cap-8,"oversize");Frame f{{htonl(id),htonl(uint32_t(p.size()))},&p};auto t=ns();unique_lock l(q.m);q.lock_ns+=ns()-t;
        while(!closing.load()&&f.size()>q.cap-q.used){++q.waits;q.cv.wait(l);}check(!closing.load(),"closed");bool empty=!q.count;q.slots[(q.head+q.count)%q.slots.size()]=f;++q.count;q.used+=f.size();q.peak=max(q.peak,q.used);l.unlock();if(empty)signal();}
    // Call finish only after all producers have returned.
    void finish(){close_all();if(worker.joinable())worker.join();check(!failed.load(),"send failed");}
    array<uint64_t,3> stats(){array<uint64_t,3> a{};for(auto& q:qs){a[0]+=q->peak;a[1]+=q->lock_ns;a[2]+=q->waits;}return a;}
};
static void tcp(int f[2]){FD l{socket(AF_INET,SOCK_STREAM,0)};check(l.fd>=0,"socket");sockaddr_in a{};a.sin_family=AF_INET;a.sin_addr.s_addr=htonl(INADDR_LOOPBACK);check(!bind(l.fd,(sockaddr*)&a,sizeof(a))&&!listen(l.fd,1),"bind");socklen_t z=sizeof(a);check(!getsockname(l.fd,(sockaddr*)&a,&z),"name");FD tx{socket(AF_INET,SOCK_STREAM,0)};check(tx.fd>=0&&!connect(tx.fd,(sockaddr*)&a,z),"connect");FD rx{accept(l.fd,nullptr,nullptr)};check(rx.fd>=0,"accept");int one=1,b=16384;timeval t{3,0};check(!setsockopt(tx.fd,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one))&&!setsockopt(tx.fd,SOL_SOCKET,SO_SNDBUF,&b,sizeof(b))&&!setsockopt(rx.fd,SOL_SOCKET,SO_RCVTIMEO,&t,sizeof(t)),"opts");f[0]=tx.fd;f[1]=rx.fd;tx.fd=rx.fd=-1;}
static void trial(unsigned seed,unsigned rep,unsigned producers,bool agg,bool slow){constexpr size_t count=24000,cap=65536;mt19937 rng(seed);vector<vector<unsigned char>> data(count);size_t bytes=0;for(auto& p:data){constexpr size_t sizes[]={0,64,128,512,2048,8192};p.resize(sizes[rng()%6]);for(auto& b:p)b=rng();bytes+=p.size();}
    vector<atomic<uint64_t>> entered(count);vector<uint64_t> lat;lat.reserve(count);vector<size_t> next(producers);for(unsigned p=0;p<producers;++p)next[p]=p;
    int f[2];tcp(f);FD tx{f[0]},rx{f[1]};Sender s(tx.fd,cap,producers,32,agg);exception_ptr err;mutex em;auto fail=[&]{lock_guard l(em);if(!err)err=current_exception();shutdown(tx.fd,SHUT_RDWR);shutdown(rx.fd,SHUT_RDWR);};size_t rc=0;auto t=ns();auto cpu=clock();
    thread reader([&]{try{vector<unsigned char> buf(cap);size_t end=0;for(;;){check(end<buf.size(),"full decoder");auto r=recv(rx.fd,buf.data()+end,buf.size()-end,0);++rc;if(r<0&&errno==EINTR)continue;check(r>=0,"recv");if(!r){check(!end,"partial EOF");break;}end+=r;size_t pos=0;while(end-pos>=8){uint32_t h[2];memcpy(h,buf.data()+pos,8);size_t id=ntohl(h[0]),len=ntohl(h[1]);check(id<count&&len<=8192,"invalid header");if(end-pos<8+len)break;check(id==next[id%producers]&&len==data[id].size()&&equal(data[id].begin(),data[id].end(),buf.data()+pos+8),"invalid data/order");next[id%producers]+=producers;auto start=entered[id].load(memory_order_acquire);check(start>0,"timestamp");lat.push_back(ns()-start);pos+=8+len;if(slow&&lat.size()%64==0)this_thread::sleep_for(chrono::microseconds(500));}end-=pos;if(end&&pos)memmove(buf.data(),buf.data()+pos,end);}check(lat.size()==count,"missing frames");}catch(...){fail();}});
    vector<thread> ps;for(unsigned p=0;p<producers;++p)ps.emplace_back([&,p]{try{for(size_t i=p;i<count;i+=producers){entered[i].store(ns(),memory_order_release);s.submit(p,i,data[i]);}}catch(...){fail();}});for(auto& p:ps)p.join();try{s.finish();}catch(...){fail();}reader.join();if(err)rethrow_exception(err);double sec=(ns()-t)/1e9,cn=double(clock()-cpu)/CLOCKS_PER_SEC*1e9/bytes;sort(lat.begin(),lat.end());auto st=s.stats();check(st[0]<=cap,"capacity");cout<<seed<<','<<rep<<','<<producers<<','<<agg<<','<<slow<<','<<count<<','<<bytes/sec/1e6<<','<<cn<<','<<lat[count*99/100-1]/1000.0<<','<<st[0]<<','<<double(st[1])/count<<','<<st[2]<<','<<s.calls<<','<<rc<<','<<s.batches<<','<<s.mixed_batches<<'\n';}
static void disconnect(bool agg){int f[2];check(!socketpair(AF_UNIX,SOCK_STREAM,0,f),"pair");FD tx{f[0]},rx{f[1]};int b=4096;check(!setsockopt(tx.fd,SOL_SOCKET,SO_SNDBUF,&b,sizeof(b)),"sndbuf");vector<unsigned char> fill(4096),p(56);for(;;){auto n=send(tx.fd,fill.data(),fill.size(),MSG_DONTWAIT|MSG_NOSIGNAL);if(n<0&&errno==EINTR)continue;if(n<0){check(errno==EAGAIN||errno==EWOULDBLOCK,"fill");break;}check(n>0,"fill zero");}Sender s(tx.fd,256,4,32,agg);for(unsigned i=0;i<4;++i)s.submit(i,i,p);atomic<unsigned> started{0},done{0},bad{0};vector<thread> v;for(unsigned i=0;i<4;++i)v.emplace_back([&,i]{++started;try{s.submit(i,i+4,p);}catch(...){++bad;}++done;});while(started!=4)this_thread::yield();this_thread::sleep_for(chrono::milliseconds(30));bool stalled=done==0;shutdown(rx.fd,SHUT_RDWR);for(auto& t:v)t.join();bool failure=false;try{s.finish();}catch(...){failure=true;}check(stalled&&bad==4&&failure,"disconnect wake");}
int main(int argc,char** argv){try{if(argc>1&&string(argv[1])=="--test"){for(bool agg:{false,true})for(int i=0;i<10;i++)disconnect(agg);trial(7,0,4,false,false);trial(7,0,4,true,false);cout<<"PASS\n";return 0;}cout<<"seed,rep,producers,aggregate,slow,messages,MBps,cpu_ns_byte,p99_us,sum_shard_peaks,lock_ns_message,waits,send_calls,recv_calls,batches,mixed_batches\n";struct C{unsigned s,r,p;bool h,l;};vector<C> cases;for(unsigned s:{11,29,47})for(unsigned r=0;r<2;r++)for(unsigned p:{1,4})for(bool h:{false,true})for(bool l:{false,true})cases.push_back({s,r,p,h,l});mt19937 order(2718);shuffle(cases.begin(),cases.end(),order);for(auto c:cases)trial(c.s,c.r,c.p,c.h,c.l);return 0;}catch(const exception& e){cerr<<e.what()<<'\n';return 1;}}

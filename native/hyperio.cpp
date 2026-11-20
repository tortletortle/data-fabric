// HyperFlow V0.6: bounded byte ring, TCP compatible framing, Linux/POSIX.
#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <mutex>
#include <random>
#include <stdexcept>
#include <thread>
#include <vector>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <unistd.h>
using namespace std;
using Clock = chrono::steady_clock;
static void require(bool ok, const char* msg) { if (!ok) throw runtime_error(msg); }
struct FD { int n; ~FD(){ if(n>=0) close(n); } };
struct Stats { size_t peak=0, waits=0, calls=0; };
class Ring {
    int fd; vector<unsigned char> buf; vector<size_t> lens;
    size_t head=0, tail=0, used=0, first=0, queued=0, limit, chunk;
    mutex m; condition_variable cv; bool closing=false, failed=false;
    Stats stats; thread worker;
    void copy_in(const void* p, size_t n) {
        if(!n) return;
        size_t a=min(n,buf.size()-tail); memcpy(buf.data()+tail,p,a);
        if(n>a) memcpy(buf.data(),static_cast<const unsigned char*>(p)+a,n-a);
        tail=(tail+n)%buf.size();
    }
    void run() noexcept {
        for(;;){
            size_t bytes=0, count=0, pos=0;
            { unique_lock<mutex> l(m); cv.wait(l,[&]{return queued||closing;});
              if(!queued) break;
              count=min(queued,limit); pos=head;
              for(size_t i=0;i<count;i++) bytes+=lens[(first+i)%lens.size()]; }
            size_t left=bytes;
            while(left){
                size_t n=min(left,chunk), a=min(n,buf.size()-pos);
                iovec v[2]={{buf.data()+pos,a},{buf.data(),n-a}};
                msghdr msg{}; msg.msg_iov=v; msg.msg_iovlen=n>a?2:1;
                ssize_t k=sendmsg(fd,&msg,MSG_NOSIGNAL); ++stats.calls;
                if(k<0 && errno==EINTR) continue;
                if(k<=0){ lock_guard<mutex> l(m); failed=closing=true; cv.notify_all(); shutdown(fd,SHUT_RDWR); return; }
                left-=size_t(k); pos=(pos+size_t(k))%buf.size();
            }
            { lock_guard<mutex> l(m); head=pos; used-=bytes; first=(first+count)%lens.size(); queued-=count; cv.notify_all(); }
        }
        shutdown(fd,SHUT_WR);
    }
public:
    Ring(int sock,size_t cap,size_t batch,size_t max_chunk=SIZE_MAX):fd(sock),buf(cap),lens(cap/8),limit(batch),chunk(max_chunk){
        require(cap>=8 && batch && chunk,"invalid capacity or batch");
        timeval timeout{3,0}; require(setsockopt(fd,SOL_SOCKET,SO_SNDTIMEO,&timeout,sizeof(timeout))==0,"send timeout");
        worker=thread([this]{run();});
    }
    ~Ring(){ stop(); }
    void submit(uint32_t id,const unsigned char* p,size_t n){
        require(n<=UINT32_MAX && n<=buf.size()-8 && (p||!n),"frame too large or invalid");
        size_t size=n+8; unique_lock<mutex> l(m);
        while(!closing && size>buf.size()-used){++stats.waits; cv.wait(l);}
        require(!closing,"sender closed or failed");
        uint32_t h[2]={htonl(id),htonl(uint32_t(n))}; copy_in(h,8); copy_in(p,n);
        lens[(first+queued)%lens.size()]=size; ++queued; used+=size;
        stats.peak=max(stats.peak,used); cv.notify_all();
    }
    void stop() noexcept { {lock_guard<mutex> l(m); closing=true; cv.notify_all();} if(worker.joinable())worker.join(); }
    Stats finish(){stop(); require(!failed && !used,"send failed");return stats;}
};
static void exact(int fd,void* p,size_t n){auto* b=static_cast<unsigned char*>(p);while(n){ssize_t k=recv(fd,b,n,0);if(k<0&&errno==EINTR)continue;require(k>0,"truncated frame");b+=k;n-=size_t(k);}}
static void pair_tcp(int* f){FD l{socket(AF_INET,SOCK_STREAM,0)};require(l.n>=0,"socket");sockaddr_in a{};a.sin_family=AF_INET;a.sin_addr.s_addr=htonl(INADDR_LOOPBACK);require(bind(l.n,(sockaddr*)&a,sizeof(a))==0&&listen(l.n,1)==0,"bind/listen");socklen_t z=sizeof(a);require(getsockname(l.n,(sockaddr*)&a,&z)==0,"getsockname");FD tx{socket(AF_INET,SOCK_STREAM,0)};require(tx.n>=0&&connect(tx.n,(sockaddr*)&a,z)==0,"connect");FD rx{accept(l.n,nullptr,nullptr)};require(rx.n>=0,"accept");int b=16384;timeval t{3,0};require(setsockopt(tx.n,SOL_SOCKET,SO_SNDBUF,&b,sizeof(b))==0&&setsockopt(rx.n,SOL_SOCKET,SO_RCVTIMEO,&t,sizeof(t))==0,"sockopt");f[0]=tx.n;f[1]=rx.n;tx.n=rx.n=-1;}
static vector<unsigned char> payload(mt19937& rng){constexpr size_t sizes[]={0,64,128,512,2048,8192};vector<unsigned char> p(sizes[rng()%6]);for(auto& b:p)b=static_cast<unsigned char>(rng());return p;}
static void probe(unsigned seed,size_t batch,bool slow,size_t count,size_t cap=65536,size_t chunk=SIZE_MAX,bool nodelay=false,unsigned rep=0){
    int f[2];pair_tcp(f);FD tx{f[0]},rx{f[1]};int nd=nodelay;require(setsockopt(tx.n,IPPROTO_TCP,TCP_NODELAY,&nd,sizeof(nd))==0,"nodelay");size_t total=0;exception_ptr error;
    Ring s(tx.n,cap,batch,chunk);auto start=Clock::now();
    thread reader([&]{try{mt19937 rng(seed);array<unsigned char,8192> b{};for(size_t i=0;i<count;i++){uint32_t h[2];exact(rx.n,h,8);auto p=payload(rng);require(ntohl(h[0])==i&&ntohl(h[1])==p.size(),"header mismatch");exact(rx.n,b.data(),p.size());require(equal(p.begin(),p.end(),b.begin()),"payload mismatch");if(slow&&i%32==0)this_thread::sleep_for(chrono::milliseconds(1));}unsigned char e;require(recv(rx.n,&e,1,0)==0,"missing EOF");}catch(...){error=current_exception();shutdown(rx.n,SHUT_RDWR);}});
    Stats st;try{mt19937 rng(seed);for(size_t i=0;i<count;i++){auto p=payload(rng);total+=p.size();s.submit(uint32_t(i),p.data(),p.size());}st=s.finish();}catch(...){shutdown(tx.n,SHUT_RDWR);s.stop();reader.join();throw;}
    reader.join();if(error)rethrow_exception(error);require(st.peak<=cap,"capacity exceeded");double sec=chrono::duration<double>(Clock::now()-start).count();
    cout<<seed<<','<<batch<<','<<slow<<','<<nodelay<<','<<rep<<','<<count<<','<<total/sec/1e6<<','<<st.peak<<','<<st.waits<<','<<st.calls<<'\n';
}
static void failure_test(){int f[2];require(socketpair(AF_UNIX,SOCK_STREAM,0,f)==0,"socketpair");FD tx{f[0]},rx{f[1]};Ring s(tx.n,64,4);unsigned char p[64]{};bool oversized=false;try{s.submit(0,p,64);}catch(const runtime_error&){oversized=true;}require(oversized,"oversize accepted");shutdown(rx.n,SHUT_RDWR);bool failed=false;try{s.submit(0,p,20);s.finish();}catch(const runtime_error&){failed=true;}require(failed,"disconnect not detected");}
int main(int argc,char** argv){try{
    if(argc>1&&string(argv[1])=="--test"){failure_test();probe(7,7,false,500,8203,13);cout<<"PASS\n";return 0;}
    cout<<"seed,batch,slow,nodelay,rep,messages,MBps,peak_bytes,waits,send_calls\n";
    struct Case{unsigned seed;size_t batch;bool slow,nd;unsigned rep;};vector<Case> cases;
    for(unsigned rep=0;rep<3;rep++)for(bool nd:{false,true})for(bool slow:{false,true})for(size_t batch:{1,32})for(unsigned seed:{11,29,47})cases.push_back({seed,batch,slow,nd,rep});
    mt19937 order(20261120);shuffle(cases.begin(),cases.end(),order);
    for(auto c:cases)probe(c.seed,c.batch,c.slow,12000,65536,SIZE_MAX,c.nd,c.rep);
    return 0;
}catch(const exception& e){cerr<<e.what()<<'\n';return 1;}}

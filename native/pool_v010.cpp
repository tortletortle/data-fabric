// HyperFlow V0.10: static per-shard quota vs bounded shared byte pool with soft reservations.
// Linux/POSIX, C++20. Payload bytes are borrowed from pre-generated immutable vectors.
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <deque>
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
using namespace std;
using Clock = chrono::steady_clock;
static uint64_t now_ns(){ return chrono::duration_cast<chrono::nanoseconds>(Clock::now().time_since_epoch()).count(); }
static void require(bool v,const char* m){ if(!v) throw runtime_error(m); }
struct FD { int fd; ~FD(){ if(fd>=0) close(fd); } };
struct Frame { array<uint32_t,2> header; const vector<unsigned char>* payload; size_t bytes() const { return 8+payload->size(); } };
struct Shard {
    mutex m;
    deque<Frame> queue;
    size_t used=0, peak=0;
    atomic<unsigned> waiters{0};
    uint64_t waits=0, wait_ns=0, lock_ns=0, borrowed_admits=0, borrowed_bytes=0;
};
class Sender {
    int fd_;
    size_t cap_, base_, batch_, shard_limit_;
    vector<unique_ptr<Shard>> shards_;
    atomic<size_t> global_used_{0}, global_peak_{0};
    atomic<uint64_t> work_epoch_{0};
    atomic<bool> closing_{false}, failed_{false};
    thread worker_;
    mutex wake_m_;
    condition_variable wake_cv_;
    uint64_t wake_epoch_=0;
    static void update_peak(atomic<size_t>& peak,size_t value){ size_t old=peak.load(memory_order_relaxed); while(old<value&&!peak.compare_exchange_weak(old,value,memory_order_relaxed)){} }
    bool quota_ok_locked(size_t idx,size_t bytes) const {
        const auto& own=*shards_[idx];
        if(own.used+bytes<=base_) return true;
        if(own.used+bytes>shard_limit_) return false;
        for(size_t i=0;i<shards_.size();++i) if(i!=idx && shards_[i]->waiters.load(memory_order_acquire)>0) return false;
        return true;
    }
    bool capacity_possible(size_t idx,size_t bytes){
        auto& q=*shards_[idx];
        lock_guard<mutex> l(q.m);
        return !closing_.load() && quota_ok_locked(idx,bytes) && global_used_.load(memory_order_acquire)+bytes<=cap_;
    }
    bool reserve_global(size_t bytes){
        size_t old=global_used_.load(memory_order_relaxed);
        for(;;){ if(old+bytes>cap_) return false; if(global_used_.compare_exchange_weak(old,old+bytes,memory_order_acq_rel)){ update_peak(global_peak_,old+bytes); return true; } }
    }
    void wake_producers(){
        { lock_guard<mutex> l(wake_m_); ++wake_epoch_; }
        wake_cv_.notify_all();
    }
    void wake_worker(){ work_epoch_.fetch_add(1,memory_order_release); work_epoch_.notify_one(); }
    void fail_all(){
        failed_.store(true,memory_order_release);
        closing_.store(true,memory_order_release);
        shutdown(fd_,SHUT_RDWR);
        wake_worker();
        wake_producers();
    }
    void worker_loop(){
        size_t cursor=0;
        vector<pair<size_t,Frame>> batch; batch.reserve(batch_);
        vector<size_t> bytes_by_shard(shards_.size(),0);
        vector<iovec> iov; iov.reserve(batch_*2);
        for(;;){
            batch.clear(); fill(bytes_by_shard.begin(),bytes_by_shard.end(),0);
            for(size_t j=0;j<shards_.size() && batch.size()<batch_;++j){
                size_t idx=(cursor+j)%shards_.size(); auto& q=*shards_[idx];
                lock_guard<mutex> l(q.m);
                while(!q.queue.empty() && batch.size()<batch_){
                    Frame f=q.queue.front(); q.queue.pop_front();
                    bytes_by_shard[idx]+=f.bytes(); batch.emplace_back(idx,f);
                }
            }
            if(batch.empty()){
                if(closing_.load(memory_order_acquire)) break;
                auto e=work_epoch_.load(memory_order_acquire);
                // Recheck after reading the epoch to avoid a lost wakeup.
                bool any=false;
                for(auto& qp:shards_){ lock_guard<mutex> l(qp->m); if(!qp->queue.empty()){any=true;break;} }
                if(any) continue;
                if(closing_.load(memory_order_acquire)) continue;
                work_epoch_.wait(e,memory_order_acquire);
                continue;
            }
            iov.clear();
            for(auto& item:batch){
                Frame& f=item.second;
                iov.push_back({f.header.data(),8});
                if(!f.payload->empty()) iov.push_back({const_cast<unsigned char*>(f.payload->data()),f.payload->size()});
            }
            size_t pos=0;
            while(pos<iov.size()){
                msghdr msg{}; msg.msg_iov=iov.data()+pos; msg.msg_iovlen=iov.size()-pos;
                ssize_t n=sendmsg(fd_,&msg,MSG_NOSIGNAL); ++send_calls_;
                if(n<0 && errno==EINTR) continue;
                if(n<=0){ fail_all(); return; }
                size_t consumed=size_t(n);
                while(pos<iov.size() && consumed>=iov[pos].iov_len){ consumed-=iov[pos].iov_len; ++pos; }
                if(consumed){ iov[pos].iov_base=static_cast<unsigned char*>(iov[pos].iov_base)+consumed; iov[pos].iov_len-=consumed; }
            }
            for(size_t i=0;i<shards_.size();++i){
                if(!bytes_by_shard[i]) continue;
                auto& q=*shards_[i];
                bool accounting_error=false;
                { lock_guard<mutex> l(q.m); if(q.used<bytes_by_shard[i]) accounting_error=true; else q.used-=bytes_by_shard[i]; }
                if(accounting_error){ fail_all(); return; }
                global_used_.fetch_sub(bytes_by_shard[i],memory_order_acq_rel);
            }
            cursor=(batch.back().first+1)%shards_.size();
            ++batches_;
            if(batch.size()>1) ++multi_frame_batches_;
            wake_producers();
        }
        shutdown(fd_,SHUT_WR);
    }
public:
    uint64_t send_calls_=0, batches_=0, multi_frame_batches_=0;
    Sender(int fd,size_t cap,size_t shards,size_t batch,size_t loan_factor):fd_(fd),cap_(cap),base_(shards?cap/shards:0),batch_(batch),shard_limit_(shards?min(cap,(cap/shards)*loan_factor):0){
        require(shards>0 && cap>=shards*64 && cap%shards==0 && batch>0 && batch<=32 && loan_factor>=1,"invalid sender config");
        for(size_t i=0;i<shards;++i) shards_.push_back(make_unique<Shard>());
        timeval tv{4,0}; require(setsockopt(fd_,SOL_SOCKET,SO_SNDTIMEO,&tv,sizeof(tv))==0,"send timeout");
        worker_=thread([this]{worker_loop();});
    }
    ~Sender(){
        closing_.store(true); wake_worker(); wake_producers();
        if(worker_.joinable()) worker_.join();
    }
    // Exactly one producer owns each shard. Payload storage must outlive finish().
    void submit(size_t shard,uint32_t id,const vector<unsigned char>& payload){
        require(shard<shards_.size() && payload.size()<=UINT32_MAX,"invalid frame");
        Frame f{{htonl(id),htonl(uint32_t(payload.size()))},&payload};
        const size_t bytes=f.bytes(); require(bytes<=cap_ && bytes<=shard_limit_,"frame exceeds usable capacity");
        auto& q=*shards_[shard]; bool registered=false; uint64_t wait_start=0;
        for(;;){
            uint64_t lock_start=now_ns(); unique_lock<mutex> l(q.m); q.lock_ns+=now_ns()-lock_start;
            if(closing_.load()) { l.unlock(); if(registered) q.waiters.fetch_sub(1); throw runtime_error("sender closed"); }
            bool quota=quota_ok_locked(shard,bytes);
            if(quota && reserve_global(bytes)){
                if(registered){ q.wait_ns+=now_ns()-wait_start; q.waiters.fetch_sub(1); registered=false; }
                if(q.used+bytes>base_){ ++q.borrowed_admits; q.borrowed_bytes+=bytes; }
                try { q.queue.push_back(f); } catch(...) { global_used_.fetch_sub(bytes,memory_order_acq_rel); throw; }
                q.used+=bytes; q.peak=max(q.peak,q.used);
                l.unlock(); wake_worker(); return;
            }
            l.unlock();
            if(!registered){ q.waiters.fetch_add(1,memory_order_acq_rel); ++q.waits; wait_start=now_ns(); registered=true; }
            unique_lock<mutex> wl(wake_m_); uint64_t observed=wake_epoch_;
            if(capacity_possible(shard,bytes)) continue;
            wake_cv_.wait(wl,[&]{return wake_epoch_!=observed || closing_.load();});
        }
    }
    void finish(){
        closing_.store(true,memory_order_release); wake_worker(); wake_producers();
        if(worker_.joinable()) worker_.join();
        require(!failed_.load(),"send failed");
        require(global_used_.load()==0,"unreleased global reservation");
        for(auto& qp:shards_){ lock_guard<mutex> l(qp->m); require(qp->used==0 && qp->queue.empty(),"unflushed shard"); }
    }
    struct Metrics { size_t peak=0; uint64_t waits=0,wait_ns=0,lock_ns=0,borrowed_admits=0,borrowed_bytes=0; };
    Metrics metrics() const { Metrics m; m.peak=global_peak_.load(); for(auto& qp:shards_){m.waits+=qp->waits;m.wait_ns+=qp->wait_ns;m.lock_ns+=qp->lock_ns;m.borrowed_admits+=qp->borrowed_admits;m.borrowed_bytes+=qp->borrowed_bytes;} return m; }
};
static void tcp_pair(int f[2]){
    FD l{socket(AF_INET,SOCK_STREAM,0)}; require(l.fd>=0,"socket"); sockaddr_in a{};a.sin_family=AF_INET;a.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
    require(bind(l.fd,(sockaddr*)&a,sizeof(a))==0 && listen(l.fd,1)==0,"bind/listen");socklen_t z=sizeof(a);require(getsockname(l.fd,(sockaddr*)&a,&z)==0,"getsockname");
    FD tx{socket(AF_INET,SOCK_STREAM,0)};require(tx.fd>=0 && connect(tx.fd,(sockaddr*)&a,z)==0,"connect");FD rx{accept(l.fd,nullptr,nullptr)};require(rx.fd>=0,"accept");
    int one=1,sb=16384;timeval tv{4,0};require(setsockopt(tx.fd,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one))==0 && setsockopt(tx.fd,SOL_SOCKET,SO_SNDBUF,&sb,sizeof(sb))==0 && setsockopt(rx.fd,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv))==0,"socket options");
    f[0]=tx.fd;f[1]=rx.fd;tx.fd=rx.fd=-1;
}
struct Case { unsigned seed,rep,producers,loan_factor; bool skew,slow; };
static vector<vector<unsigned char>> make_data(unsigned seed,unsigned producers,bool skew,size_t per){
    mt19937 rng(seed); vector<vector<unsigned char>> data(producers*per);
    const size_t sizes[]={64,128,512,2048,8192};
    for(unsigned p=0;p<producers;++p) for(size_t j=0;j<per;++j){auto& v=data[p*per+j];size_t n=skew?(p==0?8192:128):sizes[rng()%5];v.resize(n);for(auto& b:v)b=static_cast<unsigned char>(rng());}
    return data;
}
static void run_case(Case c){
    constexpr size_t cap=65536,per=4000,batch=32;size_t total=c.producers*per;
    auto data=make_data(c.seed,c.producers,c.skew,per);size_t payload_bytes=0;for(auto& p:data)payload_bytes+=p.size();
    vector<atomic<uint64_t>> entered(total);vector<uint64_t> latency;latency.reserve(total);vector<vector<uint64_t>> per_producer_latency(c.producers);for(auto& v:per_producer_latency)v.reserve(per);vector<size_t> next(c.producers,0);
    int f[2];tcp_pair(f);FD tx{f[0]},rx{f[1]};Sender sender(tx.fd,cap,c.producers,batch,c.loan_factor);
    exception_ptr err;mutex em;auto fail=[&]{lock_guard<mutex> l(em);if(!err)err=current_exception();shutdown(tx.fd,SHUT_RDWR);shutdown(rx.fd,SHUT_RDWR);};
    atomic<uint64_t> recv_calls{0};atomic<bool> reader_ready{false},go{false};atomic<unsigned> ready{0};uint64_t start=0;clock_t cpu=0;
    thread reader([&]{reader_ready.store(true,memory_order_release);try{
        vector<unsigned char> buf(cap);size_t end=0;
        for(;;){require(end<buf.size(),"decoder buffer full");ssize_t n=recv(rx.fd,buf.data()+end,buf.size()-end,0);++recv_calls;if(n<0&&errno==EINTR)continue;require(n>=0,"recv failed");if(!n){require(end==0,"partial trailing frame");break;}end+=size_t(n);size_t pos=0;
            while(end-pos>=8){uint32_t h[2];memcpy(h,buf.data()+pos,8);size_t id=ntohl(h[0]),len=ntohl(h[1]);require(id<total && len<=8192,"invalid frame header");if(end-pos<8+len)break;size_t producer=id/per,j=id%per;require(j==next[producer],"per-producer order mismatch");require(len==data[id].size() && equal(data[id].begin(),data[id].end(),buf.data()+pos+8),"payload mismatch");++next[producer];uint64_t t=entered[id].load(memory_order_acquire);require(t>0,"missing timestamp");latency.push_back(now_ns()-t);per_producer_latency[producer].push_back(now_ns()-t);pos+=8+len;if(c.slow && latency.size()%64==0)this_thread::sleep_for(chrono::microseconds(500));}
            end-=pos;if(end&&pos)memmove(buf.data(),buf.data()+pos,end);
        }
        require(latency.size()==total,"missing frames");for(auto n:next)require(n==per,"producer incomplete");
    }catch(...){fail();}});
    vector<thread> producers;producers.reserve(c.producers);
    for(unsigned p=0;p<c.producers;++p)producers.emplace_back([&,p]{try{ready.fetch_add(1,memory_order_release);while(!go.load(memory_order_acquire))this_thread::yield();if(c.skew && p>0) this_thread::sleep_for(chrono::milliseconds(5));for(size_t j=0;j<per;++j){size_t id=p*per+j;entered[id].store(now_ns(),memory_order_release);sender.submit(p,uint32_t(id),data[id]);}}catch(...){fail();}});
    while(ready.load(memory_order_acquire)!=c.producers || !reader_ready.load(memory_order_acquire)) this_thread::yield();
    start=now_ns();cpu=clock();go.store(true,memory_order_release);
    for(auto& t:producers) t.join();
    try{sender.finish();}catch(...){fail();}
    reader.join();
    if(err) rethrow_exception(err);
    uint64_t elapsed=now_ns()-start;double seconds=double(elapsed)/1e9;double cpu_ns_byte=double(clock()-cpu)/CLOCKS_PER_SEC*1e9/payload_bytes;sort(latency.begin(),latency.end());
    auto m=sender.metrics();require(m.peak<=cap,"global memory cap exceeded");
    double worst_p99=0,best_p99=1e100;for(auto& v:per_producer_latency){sort(v.begin(),v.end());double x=v[v.size()*99/100-1]/1000.0;worst_p99=max(worst_p99,x);best_p99=min(best_p99,x);}
    cout<<c.seed<<','<<c.rep<<','<<c.producers<<','<<c.loan_factor<<','<<c.skew<<','<<c.slow<<','<<total<<','<<payload_bytes<<','<<payload_bytes/seconds/1e6<<','<<cpu_ns_byte<<','<<latency[total*50/100-1]/1000.0<<','<<latency[total*99/100-1]/1000.0<<','<<m.peak<<','<<m.waits<<','<<m.wait_ns/1e6<<','<<m.lock_ns/double(total)<<','<<worst_p99<<','<<best_p99<<','<<m.borrowed_admits<<','<<m.borrowed_bytes<<','<<sender.send_calls_<<','<<sender.batches_<<','<<sender.multi_frame_batches_<<','<<recv_calls.load()<<'\n';
}
static void disconnect_test(size_t loan_factor){
    int f[2];require(socketpair(AF_UNIX,SOCK_STREAM,0,f)==0,"socketpair");FD tx{f[0]},rx{f[1]};int sb=4096;require(setsockopt(tx.fd,SOL_SOCKET,SO_SNDBUF,&sb,sizeof(sb))==0,"sndbuf");vector<unsigned char> fill(4096),p(56);
    for(;;){ssize_t n=send(tx.fd,fill.data(),fill.size(),MSG_DONTWAIT|MSG_NOSIGNAL);if(n<0&&errno==EINTR)continue;if(n<0){require(errno==EAGAIN||errno==EWOULDBLOCK,"fill socket");break;}require(n>0,"zero fill");}
    Sender s(tx.fd,256,4,8,loan_factor);for(unsigned i=0;i<4;++i)s.submit(i,i,p);
    atomic<unsigned> started{0},done{0},failed{0};vector<thread> ts;
    for(unsigned i=0;i<4;++i)ts.emplace_back([&,i]{++started;try{s.submit(i,i+4,p);}catch(const runtime_error&){++failed;}++done;});
    while(started.load()!=4) this_thread::yield();
    this_thread::sleep_for(chrono::milliseconds(30));
    bool blocked=done.load()==0;
    shutdown(rx.fd,SHUT_RDWR);
    for(auto& t:ts) t.join();
    bool finish_failed=false;
    try{s.finish();}catch(const runtime_error&){finish_failed=true;}
    require(blocked&&failed.load()==4&&finish_failed,"disconnect failed to wake blocked producers");
}
int main(int argc,char** argv){try{
    if(argc>1 && string(argv[1])=="--test"){
        disconnect_test(1);disconnect_test(2);disconnect_test(4);
        run_case({7,0,4,1,false,false});run_case({7,0,4,2,false,false});run_case({7,0,4,4,false,false});
        run_case({11,0,4,1,true,false});run_case({11,0,4,2,true,false});run_case({11,0,4,4,true,false});
        cout<<"SELF_TEST_PASS\n";return 0;
    }
    cout<<"seed,rep,producers,loan_factor,skew,slow,messages,payload_bytes,MBps,cpu_ns_byte,p50_us,p99_us,global_peak_bytes,waits,wait_ms,lock_ns_message,worst_producer_p99_us,best_producer_p99_us,borrowed_admits,borrowed_bytes,send_calls,batches,multi_frame_batches,recv_calls\n";
    vector<Case> cases;
    for(unsigned seed:{11,29,47})for(unsigned rep=0;rep<2;++rep){
        for(size_t factor:{1,4})for(bool slow:{false,true})cases.push_back({seed,rep,1,unsigned(factor),false,slow});
        for(size_t factor:{1,2,4})for(bool skew:{false,true})for(bool slow:{false,true})cases.push_back({seed,rep,4,unsigned(factor),skew,slow});
    }
    mt19937 order(20261123);shuffle(cases.begin(),cases.end(),order);for(auto c:cases)run_case(c);return 0;
}catch(const exception& e){cerr<<"ERROR: "<<e.what()<<'\n';return 1;}}
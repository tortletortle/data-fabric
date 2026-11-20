// V0.7: controlled comparison of frame-at-a-time and buffered TCP decoding.
#define main v06_main
#include "hyperio.cpp"
#undef main
#include <atomic>
#include <ctime>
#include <iomanip>

static uint64_t ns(){return chrono::duration_cast<chrono::nanoseconds>(Clock::now().time_since_epoch()).count();}
static void trial(unsigned seed,size_t batch,bool buffered,unsigned producers,bool slow,unsigned rep){
    constexpr size_t count=8000, cap=65536;
    vector<vector<unsigned char>> data; data.reserve(count); mt19937 rng(seed); size_t bytes=0;
    for(size_t i=0;i<count;i++){data.push_back(payload(rng));bytes+=data.back().size();}
    vector<atomic<uint64_t>> entered(count); for(auto& t:entered)t.store(0);
    vector<uint64_t> latency;latency.reserve(count); vector<size_t> next(producers);
    for(unsigned p=0;p<producers;p++)next[p]=p;
    int f[2];pair_tcp(f);FD tx{f[0]},rx{f[1]};int one=1;
    require(!setsockopt(tx.n,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one)),"nodelay");
    Ring ring(tx.n,cap,batch);size_t calls=0;exception_ptr error;mutex em;
    auto fail=[&]{lock_guard<mutex> l(em);if(!error)error=current_exception();shutdown(tx.n,SHUT_RDWR);shutdown(rx.n,SHUT_RDWR);};
    auto recv_count=[&](unsigned char* p,size_t n){for(;;){++calls;ssize_t k=recv(rx.n,p,n,0);if(k<0&&errno==EINTR)continue;require(k>=0,"recv failed");return size_t(k);}};
    auto read=[&](unsigned char* p,size_t n){while(n){size_t k=recv_count(p,n);require(k,"truncated");p+=k;n-=k;}};
    auto check=[&](uint32_t id,size_t size,const unsigned char* p){
        require(id<count&&id==next[id%producers],"sequence mismatch");next[id%producers]+=producers;
        require(size==data[id].size()&&equal(data[id].begin(),data[id].end(),p),"payload mismatch");
        auto t=entered[id].load(memory_order_acquire);require(t>0,"missing timestamp");latency.push_back(ns()-t);
        if(slow&&latency.size()%64==0)this_thread::sleep_for(chrono::microseconds(500));
    };
    auto start=Clock::now();auto cpu=clock();
    thread reader([&]{try{
        if(buffered){vector<unsigned char> buf(cap);size_t end=0;
            for(;;){require(end<buf.size(),"decoder full");size_t k=recv_count(buf.data()+end,buf.size()-end);if(!k){require(!end,"trailing partial frame");break;}end+=k;size_t pos=0;
                while(end-pos>=8){uint32_t h[2];memcpy(h,buf.data()+pos,8);size_t len=ntohl(h[1]);require(len<=8192,"invalid length");if(end-pos<8+len)break;check(ntohl(h[0]),len,buf.data()+pos+8);pos+=8+len;}
                if(pos){end-=pos;if(end)memmove(buf.data(),buf.data()+pos,end);}
            }
        }else{array<unsigned char,8192> b{};for(size_t i=0;i<count;i++){uint32_t h[2];read(reinterpret_cast<unsigned char*>(h),8);size_t len=ntohl(h[1]);require(len<=b.size(),"invalid length");read(b.data(),len);check(ntohl(h[0]),len,b.data());}require(!recv_count(b.data(),1),"trailing bytes");}
        require(latency.size()==count,"missing frames");
    }catch(...){fail();}});
    vector<thread> senders;senders.reserve(producers);
    for(unsigned p=0;p<producers;p++)senders.emplace_back([&,p]{try{for(size_t i=p;i<count;i+=producers){entered[i].store(ns(),memory_order_release);ring.submit(uint32_t(i),data[i].data(),data[i].size());}}catch(...){fail();}});
    for(auto& t:senders){t.join();}Stats st{};try{st=ring.finish();}catch(...){fail();}
    reader.join();if(error)rethrow_exception(error);
    double sec=chrono::duration<double>(Clock::now()-start).count(),cpu_s=double(clock()-cpu)/CLOCKS_PER_SEC;
    require(st.peak<=cap,"cap exceeded");sort(latency.begin(),latency.end());
    cout<<seed<<','<<rep<<','<<batch<<','<<buffered<<','<<producers<<','<<slow<<','<<count<<','<<bytes<<','<<sec<<','<<bytes/sec/1e6<<','<<cpu_s*1e9/bytes<<','<<latency[count/2-1]/1000.0<<','<<latency[count*99/100-1]/1000.0<<','<<st.peak<<','<<st.waits<<','<<st.calls<<','<<calls<<'\n';
}
static void blocked_disconnect(){
    int f[2];require(socketpair(AF_UNIX,SOCK_STREAM,0,f)==0,"socketpair");FD tx{f[0]},rx{f[1]};int b=4096;
    require(!setsockopt(tx.n,SOL_SOCKET,SO_SNDBUF,&b,sizeof(b)),"sndbuf");unsigned char fill[4096]{};
    // Fill kernel queue before starting the ring to force a stalled sender.
    for(;;){auto n=send(tx.n,fill,sizeof(fill),MSG_DONTWAIT|MSG_NOSIGNAL);if(n<0&&errno==EINTR)continue;if(n<0){require(errno==EAGAIN||errno==EWOULDBLOCK,"fill failed");break;}require(n>0,"fill zero");}
    Ring ring(tx.n,64,1);ring.submit(0,fill,56);atomic<unsigned> started{0},done{0},failed{0};vector<thread> v;
    for(unsigned i=0;i<4;i++)v.emplace_back([&,i]{++started;try{ring.submit(i+1,fill,56);}catch(const runtime_error&){++failed;}++done;});
    while(started.load()!=4){this_thread::yield();}this_thread::sleep_for(chrono::milliseconds(30));bool stalled=done.load()==0;
    shutdown(rx.n,SHUT_RDWR);for(auto& t:v)t.join();bool finish_failed=false;try{ring.finish();}catch(const runtime_error&){finish_failed=true;}
    require(stalled&&failed.load()==4&&finish_failed,"blocked producers not released on disconnect");
}
int main(int argc,char** argv){try{
    if(argc>1&&string(argv[1])=="--test"){for(int i=0;i<10;i++)blocked_disconnect();for(bool b:{false,true})trial(7,32,b,4,false,0);cout<<"PASS\n";return 0;}
    cout<<"seed,rep,batch,buffered,producers,slow,messages,bytes,seconds,MBps,cpu_ns_per_byte,p50_us,p99_us,peak_bytes,waits,send_calls,recv_calls\n"<<setprecision(9);
    struct C{unsigned seed,rep;size_t batch;bool buffered;unsigned producers;bool slow;};vector<C> cases;
    for(unsigned r=0;r<2;r++)for(unsigned s:{11,29,47})for(size_t b:{1,32})for(bool d:{false,true})for(unsigned p:{1,4})for(bool slow:{false,true})cases.push_back({s,r,b,d,p,slow});
    mt19937 order(20261121);shuffle(cases.begin(),cases.end(),order);for(auto c:cases)trial(c.seed,c.batch,c.buffered,c.producers,c.slow,c.rep);return 0;
}catch(const exception& e){cerr<<e.what()<<'\n';return 1;}}

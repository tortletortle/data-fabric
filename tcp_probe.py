"""TCP loopback microbenchmark, Python stdlib. Tests data path only, not HF-DB4 scheduling."""
import socket,threading,time,struct,random,hashlib,csv,statistics
from pathlib import Path
H=struct.Struct('!II')

def make_frames(n,seed):
 r=random.Random(seed); sizes=(64,128,512,2048,8192);frames=[];digest=hashlib.blake2b();payload=0
 for i in range(n):
  s=r.choices(sizes,(20,30,30,18,2))[0]; p=r.randbytes(s);frame=H.pack(i,s)+p;frames.append(frame);digest.update(frame);payload+=s
 return frames,digest.digest(),payload

def gather_send(sock,buffers):
 views=[memoryview(x) for x in buffers];calls=0
 while views:
  n=sock.sendmsg(views);calls+=1
  if n<=0:raise ConnectionError('sendmsg returned zero')
  while views and n>=len(views[0]):n-=len(views[0]);views.pop(0)
  if n:views[0]=views[0][n:]
 return calls

def run(frames,expected,payload,mode,batch):
 listener=socket.socket();listener.settimeout(20);listener.bind(('127.0.0.1',0));listener.listen(1);result={};ready=threading.Event()
 def recv():
  try:
   with listener.accept()[0] as conn:
    conn.settimeout(20);ready.set();digest=hashlib.blake2b();buf=bytearray();seq=0;count=0
    while True:
     chunk=conn.recv(65536)
     if not chunk:break
     digest.update(chunk);buf.extend(chunk);offset=0
     while len(buf)-offset>=H.size:
      i,s=H.unpack_from(buf,offset)
      if s>8192:raise ValueError('bad frame size')
      if len(buf)-offset<H.size+s:break
      if i!=seq:raise ValueError('sequence mismatch')
      seq+=1;count+=s;offset+=H.size+s
     if offset:del buf[:offset]
    if buf or seq!=len(frames) or count!=payload or digest.digest()!=expected:raise ValueError('data integrity mismatch')
    result.update(frames=seq,received_bytes=count,verified=True)
  except BaseException as exc:result['error']=repr(exc);ready.set()
 th=threading.Thread(target=recv);th.start()
 try:
  with socket.create_connection(listener.getsockname(),timeout=20) as sock:
   sock.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
   if not ready.wait(20):raise TimeoutError('receiver not ready')
   start=time.perf_counter();cpu=time.process_time();calls=0
   for i in range(0,len(frames),batch):
    part=frames[i:i+batch]
    if mode=='gather':calls+=gather_send(sock,part)
    else:sock.sendall(b''.join(part) if batch>1 else part[0]);calls+=1
   sock.shutdown(socket.SHUT_WR);th.join(20);elapsed=time.perf_counter()-start;cpu=time.process_time()-cpu
   if th.is_alive() or not result.get('verified'):raise RuntimeError(result)
 finally:listener.close()
 return dict(mode=mode,batch=batch,wall_s=elapsed,cpu_s=cpu,payload_MBps=payload/elapsed/1e6,messages_per_s=len(frames)/elapsed,send_api_calls=calls,cpu_ns_per_byte=cpu*1e9/payload,**result)

def main():
 rows=[];modes=[('sendall',1),('sendall',16),('sendall',64),('gather',16),('gather',64)]
 for seed in (221,337,449):
  frames,digest,payload=make_frames(30000,seed);order=modes.copy();random.Random(seed).shuffle(order)
  for m,b in order:rows.append(dict(seed=seed,**run(frames,digest,payload,m,b)))
 p=Path(__file__).with_name('tcp_probe.csv')
 with p.open('w',newline='') as f:
  wr=csv.DictWriter(f,fieldnames=rows[0]);wr.writeheader();wr.writerows(rows)
 for m,b in modes:
  rr=[x for x in rows if x['mode']==m and x['batch']==b];print(m,b,*[round(statistics.median(x[k] for x in rr),3) for k in ('payload_MBps','messages_per_s','cpu_ns_per_byte','send_api_calls')])
 print('verified',len(rows),sum(x['frames'] for x in rows))
if __name__=='__main__':main()

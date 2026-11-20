"""Standalone discrete-event simulation; all times microseconds. Python 3 stdlib only."""
import random,csv,heapq,math,statistics
from collections import deque
from pathlib import Path
DURATION=100_000
# rates in messages per microsecond, urgent fraction
WORKLOADS={'light':[(100000,.02,.15)],'steady':[(100000,.18,.15)],'burst':[(10000,.035,.12),(12000,.58,.18),(78000,.06,.12)],'mixed':[(25000,.07,.10),(25000,.24,.25),(25000,.035,.08),(25000,.28,.20)],'overload':[(100000,.65,.25)]}
SEEDS=[101,197,277,389,503]
POLICIES=[('fifo20',False,20,128,'none'),('fifo80',False,80,32,'none'),('fifo_immediate',False,0,1,'none'),('edf20',True,20,128,'none'),('edf80',True,80,128,'none'),('edf_cap35',True,20,128,'35'),('edf_cap60',True,20,128,'60'),('hfdb2',True,80,128,'adaptive')]

def generate(name,seed):
 r=random.Random(seed); out=[]; start=0
 for duration,rate,frac in WORKLOADS[name]:
  t=start+r.expovariate(rate); end=start+duration
  while t<end:
   urgent=r.random()<frac; size=r.choice((64,128,256)) if urgent else r.choices((128,512,2048,8192), (30,40,25,5))[0]
   out.append((t,t+(100 if urgent else 1000),size,urgent));t+=r.expovariate(rate)
  start=end
 return out

def simulate(msgs,spec):
 name,edf,wait,max_n,cap=spec
 q=[] if edf else deque(); i=0;t=0.; n=len(msgs); busy=0.; batches=0; peak=0; lat=[]; um=nm=u=nn=onbytes=0; pending_bytes=0; span=0.; urgent_recent=deque()
 def push(item):
  nonlocal pending_bytes,peak
  a,d,s,urg=item; pending_bytes+=s
  if edf: heapq.heappush(q,(d,a,s,urg))
  else: q.append((d,a,s,urg))
  if urg: urgent_recent.append(a)
  peak=max(peak,len(q))
 def pop(): return heapq.heappop(q) if edf else q.popleft()
 while i<n or q:
  if not q and i<n: t=max(t,msgs[i][0])
  while i<n and msgs[i][0]<=t: push(msgs[i]);i+=1
  if not q: continue
  # General oldest-head deadline approximation for FIFO; EDF actual earliest.
  d,a,s,urg=q[0]; elapsed=t-a
  while urgent_recent and urgent_recent[0]<t-1000:urgent_recent.popleft()
  # In dynamic strategy cap batch to 40us when urgent arrivals recent; otherwise 80us.
  bound=(40 if len(urgent_recent)>=8 else 80) if cap=='adaptive' else (float(cap) if cap!='none' else float('inf'))
  # For adaptive, send by estimated service margin, not merely count/timeout.
  est=min(bound,8+.4*min(len(q),max_n)+min(pending_bytes,65536)/1000)
  trigger=min(a+wait,d-est-8) if cap=='adaptive' else a+wait
  # Avoid accumulating when FIFO/EDF reaches queue size, byte cap, or time cap.
  if len(q)<max_n and pending_bytes<65536 and (8+.4*len(q)+pending_bytes/1000)<bound and t<trigger and i<n:
   t=min(trigger,msgs[i][0]);continue
  batch=[];b=0;end_service=8.;earliest=float('inf')
  while q and len(batch)<max_n:
   d,a,s,urg=q[0]; nxt=8+.4*(len(batch)+1)+(b+s)/1000
   # Only adaptive enforces current messages' deadline bound; permit one item if hopeless.
   if batch and (b+s>65536 or nxt>bound or (cap=='adaptive' and t+nxt>min(earliest,d) and t+end_service<=earliest)):break
   batch.append(pop());pending_bytes-=s;b+=s;end_service=nxt;earliest=min(earliest,d)
  assert batch
  t+=end_service;busy+=end_service;batches+=1
  for d,a,s,urg in batch:
   l=t-a;lat.append(l);miss=t>d+1e-9
   if urg:u+=1;um+=miss
   else:nn+=1;nm+=miss
   if not miss:onbytes+=s
  span=t
 assert len(lat)==n and pending_bytes==0
 lat.sort();return dict(messages=n,urgent=u,urgent_miss_pct=100*um/u,normal_miss_pct=100*nm/nn,p99_us=lat[math.ceil(.99*n)-1],batches=batches,busy_pct=100*busy/max(DURATION,span),ontime_mbps=onbytes*8/max(DURATION,span),drain_us=max(0,span-DURATION),peak_queue=peak)

def main():
 rows=[]
 for w in WORKLOADS:
  for seed in SEEDS:
   msgs=generate(w,seed)
   for spec in POLICIES:rows.append(dict(workload=w,seed=seed,policy=spec[0],**simulate(msgs,spec)))
 p=Path(__file__).with_name('hfdb2_results.csv');
 with p.open('w',newline='') as f:
  wr=csv.DictWriter(f,fieldnames=rows[0]);wr.writeheader();wr.writerows(rows)
 print('runs',len(rows))
 for w in WORKLOADS:
  print(w)
  for spec in POLICIES:
   rr=[x for x in rows if x['workload']==w and x['policy']==spec[0]]
   print(spec[0],*[round(statistics.mean(x[k] for x in rr),2) for k in ('urgent_miss_pct','normal_miss_pct','p99_us','ontime_mbps','busy_pct')])
if __name__=='__main__':main()

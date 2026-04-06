import subprocess,json,time,sys

N1="https://zipfs-production.up.railway.app"
N2="https://zipfs-node2-production.up.railway.app"
N3="https://zipfs-node3-production.up.railway.app"
NS=[N1,N2,N3]
NM=["Node1","Node2","Node3"]

def up(u,c,fn):
    r=subprocess.run(["curl","-s","--max-time","15","-X","POST","-F","file=@-;filename="+fn,u+"/api/v0/add"],input=c.encode(),capture_output=True)
    out=r.stdout.decode().strip()
    if not out:
        print("  ERROR: empty response from "+u)
        return None
    try:
        return json.loads(out)
    except:
        print("  ERROR: bad JSON: "+out[:200])
        return None

def poll(u,cid,lb,to=60):
    t0=time.time();a=0
    while time.time()-t0<to:
        a+=1
        r=subprocess.run(["curl","-s","-o","/dev/null","-w","%{http_code}","--max-time","5",u+"/ipfs/"+cid],capture_output=True)
        code=r.stdout.decode().strip()
        if code=="200":
            print("  "+lb+": PASS attempt "+str(a)+" ("+str(round(time.time()-t0,1))+"s)")
            return True
        time.sleep(2)
    print("  "+lb+": TIMEOUT ("+str(round(time.time()-t0,1))+"s)")
    return False

def gc(u,cid):
    return subprocess.run(["curl","-s","--max-time","10",u+"/ipfs/"+cid],capture_output=True).stdout.decode()

def alive(u):
    r=subprocess.run(["curl","-s","--max-time","8",u+"/api/v0/id"],capture_output=True)
    return len(r.stdout)>10

R={}

print("="*50)
print("TEST 1: Identity Check")
print("="*50)
ids=[]
for u,nm in zip(NS,NM):
    try:
        r=subprocess.run(["curl","-s","--max-time","10",u+"/api/v0/id"],capture_output=True)
        out=r.stdout.decode().strip()
        if out:
            d=json.loads(out)
            pid=d["ID"]
            ids.append(pid)
            print("  "+nm+" PeerID: "+pid)
        else:
            print("  "+nm+": NO RESPONSE (node down)")
            ids.append("DOWN_"+nm)
    except Exception as e:
        print("  "+nm+": ERROR "+str(e))
        ids.append("ERR_"+nm)
R["t1"]=len(set(ids))==3 and not any("DOWN" in i or "ERR" in i for i in ids)
print("  RESULT: "+("PASS" if R["t1"] else "PARTIAL - some nodes down"))

live_nodes=[]
live_names=[]
live_urls=[]
for u,nm in zip(NS,NM):
    if alive(u):
        live_nodes.append(u)
        live_names.append(nm)
        live_urls.append(u)
        print("  "+nm+": ALIVE")
    else:
        print("  "+nm+": DOWN")

if len(live_nodes)<2:
    print("ERROR: Need at least 2 live nodes. Aborting.")
    sys.exit(1)

print()
print("="*50)
print("TEST 2: Upload and cross-replication")
print("="*50)
R["t2"]=True
for i,src in enumerate(live_nodes):
    sname=live_names[i]
    t0=time.time()
    rsp=up(src,"test2-"+sname+"-"+str(time.time_ns()),"t2_"+sname+".txt")
    if rsp is None:
        print("  "+sname+" upload FAILED")
        R["t2"]=False
        continue
    cid=rsp["Hash"]
    print("  Uploaded to "+sname+": CID="+cid)
    for j,dst in enumerate(live_nodes):
        if j==i:
            continue
        dname=live_names[j]
        ok=poll(dst,cid,dname)
        if not ok:
            R["t2"]=False
    print("  Subtotal: "+str(round(time.time()-t0,1))+"s")

print()
print("="*50)
print("TEST 3: Content Integrity")
print("="*50)
t0=time.time()
src=live_nodes[0]
sname=live_names[0]
kn="integrity-"+str(time.time_ns())
rsp=up(src,kn,"integ.txt")
R["t3"]=True
if rsp is None:
    print("  Upload FAILED")
    R["t3"]=False
else:
    cid=rsp["Hash"]
    print("  Uploaded to "+sname+": CID="+cid)
    print("  Original: "+repr(kn))
    for j,dst in enumerate(live_nodes):
        if j==0:
            continue
        dname=live_names[j]
        ok=poll(dst,cid,dname+" avail")
        if ok:
            got=gc(dst,cid)
            if got==kn:
                print("  "+dname+" integrity: MATCH")
            else:
                print("  "+dname+" integrity: MISMATCH")
                print("    expected: "+repr(kn))
                print("    got:      "+repr(got))
                R["t3"]=False
        else:
            R["t3"]=False
print("  Total: "+str(round(time.time()-t0,1))+"s")

print()
print("="*50)
print("TEST 4: Simultaneous uploads")
print("="*50)
t0=time.time()
ts=str(time.time_ns())
cids=[]
for i,src in enumerate(live_nodes):
    sname=live_names[i]
    rsp=up(src,"sim-"+sname+"-"+ts,"sim_"+sname+".txt")
    if rsp is None:
        print("  "+sname+" upload FAILED")
        cids.append(None)
    else:
        cids.append(rsp["Hash"])
        print("  "+sname+" CID="+rsp["Hash"])

p4=0
total_checks=0
for ci in cids:
    if ci is None:
        continue
    for dst in live_nodes:
        total_checks+=1
        if poll(dst,ci,dst.split("//")[1].split(".")[0]+"/"+ci[:12]):
            p4+=1

R["t4"]=str(p4)+"/"+str(total_checks)
print("  "+R["t4"]+" passed, "+str(round(time.time()-t0,1))+"s")

print()
print("="*50)
print("FINAL SUMMARY")
print("="*50)
print("Live nodes:              "+str(len(live_nodes))+"/3 ("+", ".join(live_names)+")")
print("Test 1 (Identity):       "+("PASS" if R["t1"] else "PARTIAL"))
print("Test 2 (Replication):    "+("PASS" if R["t2"] else "FAIL"))
print("Test 3 (Integrity):      "+("PASS" if R["t3"] else "FAIL"))
print("Test 4 (Simultaneous):   "+R["t4"]+" replicated")

#%%
import matplotlib as mpl 
import matplotlib.pyplot as plt
import numpy as np
#%%
data = [
[18728.0,40.0,400.0],
[71658.0,2.0,400.0],
[74695.0,1.0,400.0],
[67056.0,4.0,400.0],
[73077.0,10.0,400.0],
[67934.0,20.0,400.0],
[107741.0,25.0,400.0],
[133146.0,1.0,400.0],
[107827.0,10.0,400.0],
[118589.0,40.0,400.0],
[108400.0,20.0,400.0],
[87820.0,25.0,400.0],
[73100.0,50.0,400.0],
[75700.0,80.0,400.0],
[73600.0,100.0,400.0],
[61370.0,50.0,400.0],
[64630.0,200.0,400.0],
[67380.0,400.0,400.0],
[60670.0,100.0,400.0],
 
]


#%%
plt_dt = ([], [])
for ent in data:
    plt_dt[0].append(ent[1])
    plt_dt[1].append(ent[0])
plt.xlabel('File Count')
plt.ylabel('Post Execute Time (Seconds)')
plt.title("Concurrency 400, Output Size = 400m per job")
plt.scatter(plt_dt[0], plt_dt[1])
#%%
TPRIMES = [2]
for tval in range(2, 1000):
    for p in TPRIMES:
        is_prime = True
        if tval % p == 0:
            is_prime = False 
            break
    if is_prime:
        TPRIMES.append(tval)
#%%
def prime_factorize(num):
    retval = []
    cur = num 
    while cur > 1:
        for p in TPRIMES:
            if cur % p == 0:
                retval.append(p)
                cur = cur / p 
                break 
    return retval 

#%% 
possible_fives = [(0, 2), (2, 0), (1, 1)]
possible_twos = [(0, 4), (4, 0), (3, 1), (1, 3), (2, 2)]
pairs = set()
for five_exp in possible_fives:
    for two_exp in possible_twos:
        pairs.add((
            5 ** five_exp[0] * 2 ** two_exp[0],
            5 ** five_exp[1] * 2 ** two_exp[1]
        ))

#%%
did = set([(1, 400), (2, 200), (4, 100), (10, 40), (20, 20), (40, 10)])
pairs.difference(did)

#%%
dt2 = '''
101500.0,4.0,400.0,50.0,1,0,1,1, 20190801121054
65160.0,4.0,400.0,150.0,56,0,58,236, 20190801133259
119150.0,4.0,400.0,90.0,3,0,1,1, 20190801154631
63170.0,4.0,400.0,70.0,64,0,60,193, 20190801171746
387560.0,4.0,400.0,70.0,48,0,46,218, 20190802123735
332900.0,4.0,400.0,50.0,33,0,31,175, 20190802134748
157790.0,4.0,400.0,10.0,2,398,2,1, 20190802153434
117670.0,4.0,400.0,30.0,1,0,0,1, 20190802171404
53930.0,4.0,400.0,70.0,25,0,27,196, 20190805121522
'''
dtm = [[float(n) for n in ln.split(',') if len(n) > 0] for ln in dt2.split('\n') if len(ln) > 0]
dtm = np.array(dtm)
dtm.transpose()

#%%
plt.xlabel('Latency (ms)')
plt.ylabel('Post Execute Time(Seconds)')
plt.title("Concurrency 400, 400m per run")
plt.scatter(dtm.transpose()[3], dtm.transpose()[0])
#%%
plt.xlabel('Latency (ms)')
plt.ylabel('Aborts')
plt.title("Concurrency 400, 400m per run")
plt.scatter(dtm.transpose()[3], dtm.transpose()[4])
#%%
plt.ylabel('Post Execute Time (Seconds)')
plt.xlabel('Aborts')
plt.scatter(dtm.transpose()[4], dtm.transpose()[0])

#%%
plt.xlabel('Latency')
plt.ylabel('Jobs With Shadow Errors')
plt.scatter(dtm.transpose()[3], dtm.transpose()[7])

#%%
plt.xlabel('Latency')
plt.ylabel('Jobs With Shadow Errors (Via Conlog)')
plt.scatter(dtm.transpose()[3], dtm.transpose()[6])

#%%
plt.scatter(dtm.transpose()[3], dtm.transpose()[4]/dtm.transpose()[6])

#%%

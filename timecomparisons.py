#%% [markdown]
#  # Delay Measurement Comparisons
#  
#  Since there's a bunch of different values that all proport to be the time it 
#  took to transfer files across the wire, we use this notebook to compare them 
#  both between themselves and against when the jobs themselves stopped. 

#%% [markdown]
# First we generate the data. 

#%%
import datagen
flags = {'xfer' : 'XferStatsLog', 'conlog' : 'con_600_run_0.log', 'endgoal' : '1562628062.0000'}
noflags = []
mydata = datagen.timecheck_run(flags, noflags)
rawdt = datagen.make_combdata(flags['xfer'], flags['conlog'])

#%%
import matplotlib as mpl 
import matplotlib.pyplot as plt

#%% [markdown]
#  This first graph is the `seconds` value reported by the client for the transfer. 
#%%
plt.hist(mydata['ClientSeconds'], bins=500)
plt.show()

#%% [markdown]
#  Next is the difference between when the job tried to finish itself and when the 
#  client reported the transfer complete. 
#%% 
plt.hist(mydata['ClientDiff'], bins=500)
plt.show()

#%% [markdown]
#  Next we split up the big and small values to see if there's clustering. 
#%%
smaller = list(filter(lambda k : k < 1000, mydata['ClientDiff']))
bigger = list(filter(lambda k : k >= 1000, mydata['ClientDiff']))
plt.hist(smaller, bins=500)
plt.savefig('smaller_clientdiff.png')
plt.hist(smaller, bins=500)
plt.show()
plt.hist(bigger, bins=500)
plt.savefig('bigger_clientdiff.png')
plt.hist(bigger, bins=500)
plt.show()

#%% [markdown]
# And now we repeat that with the data from the peer.

#%%
plt.hist(mydata['PeerSeconds'], bins=500)
plt.show()
#%%
plt.hist(mydata['PeerDiff'], bins=500)
plt.show()

#%% [markdown]
# Finally we repeat the previous tests except comparing the logged termination time
# instead of the transfer log itself. 

#%%
smaller = list(filter(lambda k : k < 1000, mydata['TermFullDiff']))
bigger = list(filter(lambda k : k >= 1000, mydata['TermFullDiff']))
plt.hist(smaller, bins=500)
plt.savefig('smaller_termfulldiff.png')
plt.hist(smaller, bins=500)
plt.show()
plt.hist(bigger, bins=500)
plt.savefig('bigger_termfulldiff.png')
plt.hist(bigger, bins=500)
plt.show()

#%%
a = len([ent for ent in mydata['ValidSource'] if ent.startswith('p')])
b = len([ent for ent in mydata['ValidSource'] if ent.startswith('c')])
print((a, b))



#%%
peerdelay = []
clientdelay = []
for idx in range(0, len(mydata['JobId'])):
    jid = mydata['JobId'][idx]
    psecs = mydata['PeerSeconds'][idx]
    pdiff = mydata['PeerDiff'][idx]
    peerdelay.append(pdiff - psecs)
    csecs = mydata['ClientSeconds'][idx]
    cdiff = mydata['ClientDiff'][idx]
    clientdelay.append(cdiff - csecs)

#%%
plt.hist(clientdelay, bins=500)
plt.show()

#%%
plt.hist(peerdelay, bins=500)
plt.show()

#%%


#%%


#%%


#%%

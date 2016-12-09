nodelist
metadataall
chunkcreate size:10 nid:0x280
namereg name:a lid:1 nid:0x280
chunkcreate size:10 nid:0x280
namereg name:b lid:2 nid:0x280
chunkcreate size:10 nid:0x280
namereg name:c lid:3 nid:0x280
chunkcreate size:10 nid:0xC241
namereg name:d lid:1 nid:0xC241
chunkcreate size:10 nid:0xC241
namereg name:e lid:2 nid:0xC241
chunkcreate size:10 nid:0xC241
namereg name:f lid:3 nid:0xC241
chunkcreate size:10 nid:0xC241
namereg name:g lid:4 nid:0xC241
chunkcreate size:10 nid:0xC241
namereg name:h lid:5 nid:0xC241
chunkcreate size:10 nid:0xC601
namereg name:i lid:1 nid:0xC601
chunkcreate size:10 nid:0xC601
namereg name:j lid:2 nid:0xC601
sleep sec:3
tmpcreate size:100 id:1
tmpcreate size:200 id:2
tmpcreate size:50 id:3
tmpcreate size:300 id:4
tmpcreate size:150 id:5
sleep sec:3
metadataall
migrate lid:1 nid:0x280 targetNid:0xC601
migrate lid:4 nid:0xC241 targetNid:0x280
sleep sec:3
metadataall
nodeshutdown nid:0xC301 hard:true
sleep sec:1
nodeshutdown nid:0xC0C1 hard:true
sleep sec:3
metadataall
quit

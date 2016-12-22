//
// Terminal script for testing the superpeer overlay
// @author: Kevin Beineke, kevin.beineke@hhu.de, 21.12.2016
//


// Determine all participating nodes
var boot = dxram.service("boot");
var nodeIds = boot.getOnlineNodeIDs();

var superpeers = new Array();
var peers = new Array();
for each(nodeId in nodeIds) {
  var curRole = boot.getNodeRole(nodeId);

  if (curRole == "superpeer") {
    superpeers.push(nodeId & 0xFFFF);
  } else if (curRole == "peer") {
    peers.push(nodeId & 0xFFFF);
  }
}

dxterm.printfln("Participating Superpeers:");
for each(superpeer in superpeers) { 
    dxterm.printfln("0x%X", superpeer);
}
dxterm.printfln("Participating Peers:");
for each(peer in peers) { 
    dxterm.printfln("0x%X", peer);
}
dxterm.cmd("metadatasummary").exec("all");


for each(peer in peers) {
    var counter = Math.floor(Math.random() * 6) + 3;
    for (i = 0; i < counter; i++) {
	dxterm.cmd("chunkcreate").exec(10, peer);
	dxterm.cmd("namereg").exec(peer, i, Math.floor(Math.random() * 10000) + 1);
    }
}

dxram.sleep(3000);
dxterm.cmd("namelist").exec();
dxterm.cmd("metadatasummary").exec("all");


var counter = Math.floor(Math.random() * 10) + 5;
for (i = 0; i < counter; i++) {
    dxterm.cmd("tmpcreate").exec(i, 100);
}


dxram.sleep(3000);
dxterm.cmd("metadatasummary").exec("all");


for each(peer in peers) {
    var counter = Math.floor(Math.random() * 3) + 1;
    for (i = 0; i < counter; i++) {
      dxterm.cmd("chunkmigrate").exec(peer, i, peers[Math.floor(Math.random() * (peers.length - 1))]);
    }
}


dxram.sleep(3000);
dxterm.cmd("metadatasummary").exec("all");


var counter = Math.floor(Math.random() * 2) + 1;
for (i = 0; i < counter; i++) {
    dxterm.cmd("nodeshutdown").exec(peers[Math.floor(Math.random() * (peers.length - 1))], true);
}


dxram.sleep(3000);
dxterm.cmd("metadatasummary").exec("all");
exit()
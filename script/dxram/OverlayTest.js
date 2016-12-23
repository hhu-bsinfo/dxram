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
dxterm.printfln("\n\n");
dxterm.cmd("metadatasummary").exec("all");


for each(peer in peers) {
    var counter = (Math.floor(Math.random() * 6) + 3) | 0;
    dxterm.printfln("Creating %d chunks on 0x%X (including nameservice register)", counter, peer);
    for (i = 0; i < counter; i++) {
	dxterm.cmd("chunkcreate").exec((Math.floor(Math.random() * 128) + 32) | 0, peer);
	dxterm.cmd("namereg").exec(peer, i, (Math.floor(Math.random() * 10000) + 1) | 0);
    }
}
dxterm.printfln("\n");

dxram.sleep(3000);
dxterm.cmd("namelist").exec();
dxterm.printfln("\n\n");
dxterm.cmd("metadatasummary").exec("all");


var counter = (Math.floor(Math.random() * 10) + 5) | 0;
dxterm.printfln("Creating %d tmp storages", counter);
for (i = 0; i < counter; i++) {
    dxterm.cmd("tmpcreate").exec((i + 1) | 0, (Math.floor(Math.random() * 1000) + 50) | 0);
}


dxram.sleep(3000);
dxterm.printfln("\n\n");
dxterm.cmd("metadatasummary").exec("all");


for each(peer in peers) {
    var counter = (Math.floor(Math.random() * 3) + 1) | 0;
    dxterm.printfln("Migrating first %d chunks from 0x%X", counter, peer);
    for (i = 0; i < counter; i++) {
      var randomPeer = peers[(Math.floor(Math.random() * peers.length)) | 0];
      dxterm.printfln("\t Migrating to 0x%X", randomPeer);
      dxterm.cmd("chunkmigrate").exec(peer, i + 1, randomPeer);
    }
}


dxram.sleep(3000);
dxterm.printfln("\n\n");
dxterm.cmd("metadatasummary").exec("all");


var counter = (Math.floor(Math.random() * 2) + 1) | 0;
dxterm.printfln("Shutting down %d superpeers", counter);
for (i = 0; i < counter; i++) {
    var randomSuperpeer = superpeers[(Math.floor(Math.random() * superpeers.length)) | 0];
    dxterm.printfln("\t Shutting down 0x%X", randomPeer);
    dxterm.cmd("nodeshutdown").exec(randomSuperpeer, true);
}


dxram.sleep(3000);
dxterm.printfln("\n\n");
dxterm.cmd("metadatasummary").exec("all");
exit()
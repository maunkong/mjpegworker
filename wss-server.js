const https = require('https');
const WebSocket = require('ws');
const { URL } = require('url');
const fs = require('fs');
const path = require('path');

const { verifyJwtFromUrl } = require('./auth');
const { resetControl, getControlOwner } = require('./mjpegcontrol');

const mjpegmap = require('./mjpegmap');  //

const HEARTBEAT_INTERVAL = 7000;

function setupPingPong(wss) {
    wss.on('connection', (ws) => { //
        ws.isAlive = true;

        ws.on('pong', () => {
            ws.isAlive = true; // 
        });
    });

    const interval = setInterval(() => {
        wss.clients.forEach((ws) => {
            if (ws.isAlive === false) {
                console.log('[WS] DEAD CLIENT TERMINATED');

                if ( ws.streamid === getControlOwner()) {
                    console.log(`[stream] ${ws.streamid} terminated, stream reset`);
                    resetControl();
                }

                return ws.terminate(); // 
            }

            ws.isAlive = false;
            ws.ping(); // 
        });

        if (getControlOwner() !== null) {
            const hasOwner = [...wss.clients].some((ws) => {
                return (
                    ws.streamid === getControlOwner()
                );
            });

            if (!hasOwner) {
                console.log('[WS] DEAD CLIENT  resetControl()');
                resetControl();
            }
        }

    }, HEARTBEAT_INTERVAL);

    wss.on('close', () => {
        clearInterval(interval); // 
    });
}



function initWSServer({ certPath, keyPath, port, getWorkerForVds, getStreamUrl  }) {

    const server = https.createServer({
        key: fs.readFileSync(path.resolve(keyPath)),
        cert: fs.readFileSync(path.resolve(certPath))
    });

    const wss = new WebSocket.Server({ noServer: true });

    setupPingPong(wss);

    server.on('upgrade', (req, socket, head) => {
        const parsedUrl = new URL(req.url, `https://${req.headers.host}`);
        const vdsNo = Number(parsedUrl.searchParams.get('vdsNo'));

        const streamUrl = getStreamUrl(vdsNo);

        if (!vdsNo || !streamUrl) {

            console.log(`wss return VDS ${vdsNo} streamurl ${streamUrl}`);

            socket.destroy();
            return;
        }

        wss.handleUpgrade(req, socket, head, (ws) => {

            req.vdsNo = vdsNo;
            req.streamid = parsedUrl.searchParams.get('streamid');

            req.streamUrl = streamUrl;

            wss.emit('connection', ws, req, vdsNo);
        });
    });


    wss.on('connection', (ws, req, vdsNo) => {
        console.log(`WSS CONNECT: VDS ${vdsNo}`);

        //ws.ping();
        //console.log(typeof ws.ping); //

        const streamid = req.streamid;
        const streamUrl = req.streamUrl;

        let user = null;
        try {
            user = verifyJwtFromUrl(req.url);
        } catch (err) {
          

            ws.close();
            return;
        }

        if (!streamid || !user?.userId) {
            

            ws.close();
            return;
        }

        // 설정
        ws.streamid = streamid;
        ws.userId = user.userId;

        console.log(`[WSS] CONNECT user=${ws.userId}, clientId=${ws.streamid}, vdsNo=${vdsNo}  streamurl=${streamUrl}`);

        const worker = getWorkerForVds(vdsNo);

        if (!worker) {
            ws.send(JSON.stringify({ type: 'error', vdsNo: vdsNo, reason: 'No 워커 available' }));

            ws.close();
            return;
        }

        if (!mjpegmap.hasStream(vdsNo)) {

            if (worker && worker.isConnected()) {

                mjpegmap.addClient(vdsNo, ws);

                console.log(`vdsNo=${vdsNo} new clientsize=${mjpegmap.getClientCount(vdsNo)}`);

                worker.send({ type: 'start_stream', vdsNo, url: streamUrl});

                worker.send({
                    type: 'update_clients',
                    vdsNo,
                    clientCount: mjpegmap.getClientCount(vdsNo),
                });

                //vdsStreamMap.get(vdsNo).active = true;
            } 
        }
        else {
            //console.log(`vdsNo=${vdsNo} clientsize=${mjpegmap.getClientCount(vdsNo)}`);

            mjpegmap.addClient(vdsNo, ws);

            console.log(`vdsNo=${vdsNo} add clientsize=${mjpegmap.getClientCount(vdsNo)}`);

            if (worker && worker.isConnected()) {

                worker.send({
                    type: 'update_clients',
                    vdsNo,
                    clientCount: mjpegmap.getClientCount(vdsNo),
                });
            }
        }

        ws.on('message', (msg) => {   
            const data = JSON.parse(msg);
            if (data.type === 'resetStream' && ws.streamid === getControlOwner()) {
                console.log(`[stream] ${ws.streamid} close, stream reset`);

                resetControl();
            }
        });

        ws.on('close', () => {

            mjpegmap.removeClient(vdsNo, ws);

  

            if (mjpegmap.getClientCount(vdsNo) === 0 ) { 

                if (worker && worker.isConnected()) {
                    worker.send({ type: 'stop_stream', vdsNo });
                }

                mjpegmap.removeStream(vdsNo);
            }
            else {
                if (worker && worker.isConnected()) {

                    worker.send({
                        type: 'update_clients',
                        vdsNo,
                        clientCount: mjpegmap.getClientCount(vdsNo),
                    });

                    console.log(`[${vdsNo}] WS Closed. Clients: ${mjpegmap.getClientCount(vdsNo)}`);
                }
            }

        });
    });

    server.listen(port, () => {
        console.log(`MJPEG WSS Server Listening on ${port}`);
    });
}


module.exports = { initWSServer };











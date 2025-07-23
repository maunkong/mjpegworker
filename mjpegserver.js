// mjpeg-ws-server.js
const axios = require('axios');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

const logger = require('./logger');

const HLS_DIR = path.join(__dirname, 'hls');

const savingInProgressSet = new Set();
const vdsStreamMap = new Map();

function insertExtXStartTag(vdsNo) { 
    const outputFile = path.join(HLS_DIR, `vds${vdsNo}.m3u8`);

    let retryCount = 0;
    const MAX_RETRY = 20;

    const tryInsert = () => {
        if (!fs.existsSync(outputFile)) {
            if (++retryCount > MAX_RETRY) {
                
                return;
            }

            return setTimeout(tryInsert, 1000);  //
        }

        fs.readFile(outputFile, 'utf8', (err, data) => {
            if (err) return console.error(`[${vdsNo}] m3u8 read error:`, err);

            if (!data.includes('#EXT-X-START')) {
                const lines = data.split('\n');
                const versionIndex = lines.findIndex(line => line.startsWith('#EXT-X-VERSION'));

                if (versionIndex >= 0) {
                    lines.splice(versionIndex + 1, 0, '#EXT-X-START:TIME-OFFSET=0.0,PRECISE=YES');
                } else {
                    lines.unshift('#EXT-X-START:TIME-OFFSET=0.0,PRECISE=YES');
                }

                fs.writeFile(outputFile, lines.join('\n'), 'utf8', (err) => {
                    if (err) console.error(`[${vdsNo}] m3u8 write error:`, err);
                   
                });
            }
        });
    };

    tryInsert();
}


function attachFFmpegToStream(vdsNo, stream) {

    if (stream.ffmpeg) {
       
        return;
    }

    if (stream.type === 'rtsp') {
        if (!stream._stdout || stream._stdout.readableEnded || stream._stdout.destroyed) {
         
            return;
        }
    }
    else { //mjpeg
        if (!stream._rawRes || stream._rawRes.readableEnded || stream._rawRes.destroyed) {
           
            return;
        }
    }

    const outputFile = path.join(HLS_DIR, `vds${vdsNo}.m3u8`);
    const outputtsFile = path.join(HLS_DIR, `vds${vdsNo}%04d.ts`);

    const ffmpeg = spawn('ffmpeg', [
        '-f', 'mjpeg',
        '-i', 'pipe:0',
        '-vf', 'scale=640:360:force_original_aspect_ratio=decrease', // 
        '-c:v', 'libx264',
        '-preset', 'ultrafast',
        '-tune', 'zerolatency',
        '-f', 'hls',
        '-hls_time', '2',
        '-hls_list_size', '250',
        '-hls_flags', 'program_date_time + independent_segments',
        '-hls_segment_filename', outputtsFile,
        outputFile
    ]);


    ffmpeg.stderr.on('data', (data) => {
        // console.log(`[${vdsNo}] ffmpeg stderr:`, data.toString());
    });

    ffmpeg.stdin.on('error', (err) => {
        console.error(`[${vdsNo}] attached streaming ffmpeg stdin error (attach):`, err.message);
    });

    ffmpeg.on('exit', (code, signal) => {
      

        if (code === null && signal === 'SIGKILL') {
           
        }
    });

    ffmpeg.on('error', (err) => {

    });

    stream.ffmpeg = ffmpeg;
}


function startMJPEGProxyStream(vdsNo, mjpegUrl, onFail, { forRecordingOnly = false } = {}) {

    const existing = vdsStreamMap.get(vdsNo);

    if (existing) {

        const hasClients = existing.clients > 0;
        const hasListeners = existing.listeners && existing.listeners.length > 0;

        if (existing.active && (hasClients || hasListeners)) {
            return; 
        }

    
        existing.active = false;
        existing.listeners = [];

        try {
            existing._rawRes?.destroy?.();
           
        } catch (e) {
            
        }

        if (/*!forRecordingOnly &&*/ existing.ffmpeg ) {
            try {
                existing.ffmpeg?.stdin?.end();
                existing.ffmpeg?.kill('SIGKILL');
            } catch (e) {
               
            }
        }

        if ( existing.ffmpegrtsp) {
            try {
                existing.ffmpegrtsp?.stdin?.end();
                existing.ffmpegrtsp?.kill('SIGKILL');
            } catch (e) {
               
            }
        }

        vdsStreamMap.delete(vdsNo); 
    }

    const streamInfo = {
        type: 'mjpeg',
        ffmpeg: null,
        ffmpegrtsp:null,
        buffer: Buffer.alloc(0),
        clients: 0,
        listeners: [],
        active: true,
        _stdout: null,          // 
        _rawRes: null,          //
        forRecordingOnly
    };

    console.log(`req vds:${vdsNo} stream current length ${vdsStreamMap.size}`);

    let ffmpeg = null;

    if (!forRecordingOnly) { 
        const outputFile = path.join(HLS_DIR, `vds${vdsNo}.m3u8`);
        const outputtsFile = path.join(HLS_DIR, `vds${vdsNo}%04d.ts`);


        /*
        144p	256×144
        240p	426×240 
        360p	640×360
        480p	854×480
        720p(HD)	1280×720
        1080p(FHD)	1920×1080*/

        ffmpeg = spawn('ffmpeg', [
            '-f', 'mjpeg',
            '-i', 'pipe:0', 
            '-vf', 'scale=640:360:force_original_aspect_ratio=decrease', // 
            '-c:v', 'libx264',
            '-preset', 'ultrafast',
            '-tune', 'zerolatency',
            '-f', 'hls',
            '-hls_time', '2',
            '-hls_list_size', '250',
            '-hls_flags', 'program_date_time + independent_segments',
            //'-hls_list_size', '50',
            //'-hls_flags', 'program_date_time',
            //'-hls_flags', 'delete_segments',
            '-hls_segment_filename', outputtsFile,
            outputFile
        ]);

        ffmpeg.stderr.on('data', (data) => {
            //console.log(`[${vdsNo}] ffmpeg:`, data.toString());
        });

        ffmpeg.stdin.on('error', (err) => {
          
            if (typeof onFail === 'function') {
  
                onFail(`stdin error: ${err.message}`);
            }

            cleanup();
        });

        ffmpeg.on('exit', (code, signal) => {
            console.log(`[${vdsNo}] ffmpeg exited. code=${code}, signal=${signal}`);

            if (code === null && signal === 'SIGKILL') {
              
            }

            cleanup();
        });

        ffmpeg.on('error', (err) => {


            if (typeof onFail === 'function') {
               
                onFail(`ffmpeg error: ${err.message}`);
            }

            cleanup();
        });

        streamInfo.ffmpeg = ffmpeg;
    }

    let inactivityTimer; // 
    let alreadyCleanedUp = false;

    const cleanup = () => {

        if (alreadyCleanedUp) return;

        if (streamInfo.lockedForSaving) {
        
            return;
        }

        alreadyCleanedUp = true;

        console.log(`[${vdsNo}] cleanup called`);

        streamInfo.active = false;
        streamInfo.listeners = [];

        if (streamInfo._rawRes?.destroy) {

            try {
                streamInfo._rawRes.destroy();
                console.log(`[${vdsNo}] cleanup _rawRes destroy`);
            } catch (e) { }
        }

        if (streamInfo.ffmpeg) {
            try {
                streamInfo.ffmpeg.stdin?.end();
                streamInfo.ffmpeg.kill('SIGKILL');

                console.log(`[${vdsNo}] cleanup  streamInfo.ffmpeg SIGKILL`);

            } catch (e) { }
        }

        vdsStreamMap.delete(vdsNo);
        clearInterval(inactivityTimer);

        //await deleteHLSFiles(vdsNo); 
        if (!streamInfo.forRecordingOnly) {

            const m3u8File = path.join(HLS_DIR, `vds${vdsNo}.m3u8`);
            const tsPattern = new RegExp(`^vds${vdsNo}\\d+\\.ts$`);
            fs.unlink(m3u8File, () => { });

            fs.readdir(HLS_DIR, (err, files) => {
                if (err) return;
                files.filter(f => tsPattern.test(f)).forEach(f => {
                    fs.unlink(path.join(HLS_DIR, f), () => { });
                });
            });

        
        }
    };

    axios({
        url: mjpegUrl,
        responseType: 'stream',
        headers: { 'User-Agent': 'Node.js' },
    }).then((res) => {

        streamInfo._rawRes = res.data; 

        const MAX_BUFFER_SIZE = 1 * 1024 * 1024;

        let lastFrameTime = Date.now();
        const INACTIVITY_TIMEOUT = 15000; 

        inactivityTimer = setInterval(() => {
            if (Date.now() - lastFrameTime > INACTIVITY_TIMEOUT) {
           
                if (typeof onFail === 'function') {
                   
                }

                //if (res.data && res.data.destroy) res.data.destroy();
                //clearInterval(inactivityTimer);
                streamInfo.lockedForSaving = false; 
                cleanup(); 
            }
        }, 1000);

        let insertedExtXStart = false;

        let lastCleanupTime = 0; 
        let endTimeout = null;

        const CLEANUP_INTERVAL = 1000; // 
        const MAX_TS_FILES = 100;
        const KEEP_TS_COUNT = 50;

        res.data.on('data', (chunk) => {

            //console.log(`[${vdsNo}] chunk ${chunk.length} bytes`);
            if (!streamInfo.active) {
             
                return;
            }

            /*if (!streamInfo.active || !ffmpeg || !ffmpeg.stdin || !ffmpeg.stdin.writable) {
                res.data.destroy?.();
                return;
            }*/

            streamInfo.buffer = Buffer.concat([streamInfo.buffer, chunk]);
            if (streamInfo.buffer.length > MAX_BUFFER_SIZE) {
                streamInfo.buffer = Buffer.alloc(0);
                return;
            }

            const start = streamInfo.buffer.indexOf(Buffer.from([0xff, 0xd8]));
            const end = streamInfo.buffer.indexOf(Buffer.from([0xff, 0xd9]));

            if (start !== -1 && end !== -1 && end > start) {

                const jpg = streamInfo.buffer.slice(start, end + 2);
                streamInfo.buffer = streamInfo.buffer.slice(end + 2);

                lastFrameTime = Date.now();
                streamInfo.lastFrameAt = lastFrameTime;

                if (endTimeout) {
                    clearTimeout(endTimeout);
                    endTimeout = null;
                   
                }

                try {
                    if (!streamInfo.forRecordingOnly && streamInfo.ffmpeg?.stdin?.writable) {
                        streamInfo.ffmpeg.stdin.write(jpg); 
                    }
                } catch (e) {
                    console.error(`[${vdsNo}] ffmpeg.stdin.write 실패:`, e.message);
                    cleanup();
                }

                try {
                    streamInfo.listeners.forEach(cb => cb(jpg)); 
                } catch (e) {
                    console.error(`[${vdsNo}] listeners error:`, e.message);
                }

                //console.log('resolution', getJpegResolution(jpg));

                /*if (!insertedExtXStart && !streamInfo.forRecordingOnly) {
                    insertedExtXStart = true;
                    insertExtXStartTag(vdsNo);
                }*/

            
                const now = Date.now();

                if (!streamInfo.forRecordingOnly && now - lastCleanupTime > CLEANUP_INTERVAL) {
                    lastCleanupTime = now;

                    const tsPattern = new RegExp(`^vds${vdsNo}\\d+\\.ts$`);
                    fs.readdir(HLS_DIR, (err, files) => {
                        if (err) return;
                        const tsFiles = files
                            .filter(f => tsPattern.test(f))
                            .sort(); // 

                        if (tsFiles.length > MAX_TS_FILES) {
                            const filesToDelete = tsFiles.slice(0, tsFiles.length - KEEP_TS_COUNT);
                            filesToDelete.forEach(file => {
                                fs.unlink(path.join(HLS_DIR, file), () => { });
                            });
                        
                        }
                    });
                }
            }
        });

        res.data.on('aborted', () => {
            console.warn(`[${vdsNo}] MJPEG stream aborted by client.`);
            cleanup();
        });

        res.data.on('end', () => {
            console.log(`[${vdsNo}] MJPEG stream ended.`);

            endTimeout = setTimeout(() => {
                if (Date.now() - streamInfo.lastFrameAt > 5000) {
                   
                    cleanup();
                }
            }, 5000);
        });

        res.data.on('error', (err) => {
            console.error(`[${vdsNo}] MJPEG stream error:`, err.message);
            cleanup();
        });

        res.data.on('close', () => {  
            console.log(`[${vdsNo}] res.data stream closed`);
            cleanup();
        });

    }).catch((err) => {
      

        if (typeof onFail === 'function') {
            console.error(`[${vdsNo}] MJPEG stream failed:`, err.message);

            onFail(err.message);
        }

        streamInfo.lockedForSaving = false; /

        cleanup();
        //vdsStreamMap.delete(vdsNo);
    });


    vdsStreamMap.set(vdsNo, streamInfo);
}



function startRtspToMjpeg(vdsNo, rtspUrl, onFail, { forRecordingOnly = false } = {}) {

    const existing = vdsStreamMap.get(vdsNo);

    if (existing) {

        const hasClients = existing.clients > 0;
        const hasListeners = existing.listeners && existing.listeners.length > 0;

        if (existing.active && (hasClients || hasListeners)) {
          
            return; 
        }


        existing.active = false;
        existing.listeners = [];

        try {
            existing._rawRes?.destroy?.();
           
        } catch (e) {
            
        }

        if (/*!forRecordingOnly &&*/ existing.ffmpeg) {
            try {
                existing.ffmpeg?.stdin?.end();
                existing.ffmpeg?.kill('SIGKILL');
            } catch (e) {
              
            }
        }

        if (existing.ffmpegrtsp) {
            try {
                existing.ffmpegrtsp?.stdin?.end();
                existing.ffmpegrtsp?.kill('SIGKILL');
            } catch (e) {
               
            }
        }

        vdsStreamMap.delete(vdsNo); //
    }

    const streamInfo = {
        type: 'rtsp',
        ffmpeg: null,
        ffmpegrtsp: null,
        buffer: Buffer.alloc(0),
        clients: 0,
        listeners: [],
        active: true,
        _stdout: null,          // 
        _rawRes: null,          //
        forRecordingOnly
    };

    const ffmpegrtsp = spawn('ffmpeg', [
        '-rtsp_transport', 'tcp',
        '-i', rtspUrl,
        '-f', 'mjpeg',
        'pipe:1'
    ]);


    let ffmpeg = null;

    if (!forRecordingOnly) {

        const outputFile = path.join(HLS_DIR, `vds${vdsNo}.m3u8`);
        const outputtsFile = path.join(HLS_DIR, `vds${vdsNo}%04d.ts`);

        ffmpeg = spawn('ffmpeg', [
            '-f', 'mjpeg',
            '-i', 'pipe:0',
            '-vf', 'scale=640:360:force_original_aspect_ratio=decrease',
            '-c:v', 'libx264',
            '-preset', 'ultrafast',
            '-tune', 'zerolatency',
            '-f', 'hls',
            '-hls_time', '2',
            '-hls_list_size', '250',
            '-hls_flags', 'program_date_time+independent_segments',
            '-hls_segment_filename', outputtsFile,
            outputFile
        ]);

        ffmpeg.stdin.on('error', err => console.error(`[${vdsNo}] RTSP ffmpeg.stdin error`, err.message));
        ffmpeg.on('exit', () => );

        streamInfo.ffmpeg = ffmpeg;
    }

    streamInfo.ffmpegrtsp = ffmpegrtsp;
    streamInfo._stdout = ffmpegrtsp.stdout;

    let lastFrameTime = Date.now();
    const INACTIVITY_TIMEOUT = 15000; 

    const MAX_BUFFER_SIZE = 1 * 1024 * 1024;

    const inactivityTimer = setInterval(() => {
        if (Date.now() - lastFrameTime > INACTIVITY_TIMEOUT) {

            if (typeof onFail === 'function') {
               
            }

            streamInfo.lockedForSaving = false;

            cleanup();
        }

    }, 1000);


    let alreadyCleanedUp = false;

    const cleanup = () => {
        if (alreadyCleanedUp) return;

        if (streamInfo.lockedForSaving) {
        
            return;
        }

        alreadyCleanedUp = true;

        streamInfo.active = false;
        streamInfo.listeners = [];

        try {
            streamInfo.ffmpegrtsp?.kill('SIGKILL');
        } catch (e) { }

        try {
            streamInfo.ffmpeg?.stdin?.end();
            streamInfo.ffmpeg?.kill('SIGKILL');
        } catch (e) { }

        vdsStreamMap.delete(vdsNo);
        clearInterval(inactivityTimer);
        
        if (!streamInfo.forRecordingOnly) {

            const m3u8File = path.join(HLS_DIR, `vds${vdsNo}.m3u8`);
            const tsPattern = new RegExp(`^vds${vdsNo}\\d+\\.ts$`);
            fs.unlink(m3u8File, () => { });

            fs.readdir(HLS_DIR, (err, files) => {
                if (err) return;
                files.filter(f => tsPattern.test(f)).forEach(f => {
                    fs.unlink(path.join(HLS_DIR, f), () => { });
                });
            });

    
        }

    };


    ffmpegrtsp.stderr.on('data', () => { });

    ffmpegrtsp.on('exit', () => {
      
        cleanup();
    });

    ffmpegrtsp.on('error', (err) => {

       

        cleanup();
    });

    ffmpegrtsp.stdin.on('error', err => {

        console.error(`[${vdsNo}] ffmpeg.stdin error`, err.message);

        cleanup();
    });

    ffmpegrtsp.on('exit', () => {

        cleanup();
    });

    let lastCleanupTime = 0; 
    const CLEANUP_INTERVAL = 1000; //
    const MAX_TS_FILES = 100;
    const KEEP_TS_COUNT = 50;

    ffmpegrtsp.stdout.on('data', (chunk) => {
        streamInfo.buffer = Buffer.concat([streamInfo.buffer, chunk]);

        if (streamInfo.buffer.length > MAX_BUFFER_SIZE) {
            streamInfo.buffer = Buffer.alloc(0);
            return;
        }

        const start = streamInfo.buffer.indexOf(Buffer.from([0xff, 0xd8]));
        const end = streamInfo.buffer.indexOf(Buffer.from([0xff, 0xd9]));

        if (start !== -1 && end !== -1 && end > start) {
            const jpg = streamInfo.buffer.slice(start, end + 2);
            streamInfo.buffer = streamInfo.buffer.slice(end + 2);

            lastFrameTime = Date.now();

            if ( streamInfo.ffmpeg && streamInfo.ffmpeg.stdin && !streamInfo.ffmpeg.stdin.destroyed && streamInfo.ffmpeg.stdin.writable ) {
                try {
                    streamInfo.ffmpeg.stdin.write(jpg);
                } catch (e) {
    
                    streamInfo.lockedForSaving = false; //
                    cleanup();
                }
            }

            try {
                streamInfo.listeners.forEach(cb => cb(jpg));  //
            } catch (e) {
                console.error(`[${vdsNo}] listeners error:`, e.message);
            }

            const now = Date.now();

            if (!streamInfo.forRecordingOnly && now - lastCleanupTime > CLEANUP_INTERVAL) {
                lastCleanupTime = now;

                const tsPattern = new RegExp(`^vds${vdsNo}\\d+\\.ts$`);
                fs.readdir(HLS_DIR, (err, files) => {
                    if (err) return;
                    const tsFiles = files
                        .filter(f => tsPattern.test(f))
                        .sort(); // 

                    if (tsFiles.length > MAX_TS_FILES) {
                        const filesToDelete = tsFiles.slice(0, tsFiles.length - KEEP_TS_COUNT);
                        filesToDelete.forEach(file => {
                            fs.unlink(path.join(HLS_DIR, file), () => { });
                        });
             
                    }
                });
            }
        }
    });

    vdsStreamMap.set(vdsNo, streamInfo);

}



function captureMJPEG(vdsNo, mjpegUrl, duration, filename, retryCount = 0) {

    if (savingInProgressSet.has(vdsNo)) {
        console.error(`[${vdsNo}] already savingInProgressSet `);

        process.send({ type: 'mjpeg_saved', vdsNo, filename, error: true });

        return;
    }

    //console.log(typeof vdsNo);

    let stream = vdsStreamMap.get(vdsNo);

    //console.log(`captureMJPEG stream length ${vdsStreamMap.size}`);

    if (!stream) { //|| !stream.active
       
        if (mjpegUrl.startsWith('rtsp://')) {

            startRtspToMjpeg(vdsNo, mjpegUrl, (errMsg) => {
          
                process.send({ type: 'mjpeg_saved', vdsNo, filename, error: true, message: errMsg });
            }, { forRecordingOnly: true }); 

            if (retryCount >= 3) {
                process.send({ type: 'mjpeg_saved', vdsNo, filename, error: true });
                return;
            }

            setTimeout(() => {
        
                captureMJPEG(vdsNo, mjpegUrl, duration, filename, retryCount + 1);
            }, 1000); // 

        } else {

            startMJPEGProxyStream(vdsNo, mjpegUrl, (errMsg) => {
          
                process.send({ type: 'mjpeg_saved', vdsNo, filename, error: true, message: errMsg });
            }, { forRecordingOnly: true }); 

            if (retryCount >= 3) {
                process.send({ type: 'mjpeg_saved', vdsNo, filename, error: true });
                return;
            }

            setTimeout(() => {
            
                captureMJPEG(vdsNo, mjpegUrl, duration, filename, retryCount + 1);
            }, 1000); // 
        }

        return;
    }

    process.send({
        type: 'status_update',
        pid: process.pid,
        vdsList: Array.from(vdsStreamMap.keys()), //
    });

    stream.lockedForSaving = true;   //
    savingInProgressSet.add(vdsNo);

    const dir = path.dirname(filename);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

    const ffmpeg = spawn('ffmpeg', [
        '-y',
        '-f', 'mjpeg',
        '-i', 'pipe:0',
        '-t', String(duration),
        '-c:v', 'libx264',
        '-preset', 'ultrafast',
        '-threads', '3',
        filename
    ]);

    const timeoutMs = (duration * 2) * 1000; //
    const timeout = setTimeout(() => {

        console.error(`[${vdsNo}] mjpeg_saved ffmpeg timeout 강제종료 ms${timeoutMs}`);

        try {
            ffmpeg.stdin?.end(); // 
            ffmpeg.kill('SIGKILL');
        } catch (e) {
            console.error(`[${vdsNo}] ffmpeg kill error:`, e.message);
        }

    }, timeoutMs); //

    let alreadyCleanedUp = false;

    const listener = (frame) => {

        if (alreadyCleanedUp || !ffmpeg.stdin?.writable || ffmpeg.killed || ffmpeg.stdin.destroyed || ffmpeg.stdin.closed ) return;

        try {
            if (ffmpeg.stdin && ffmpeg.stdin.writable && !ffmpeg.killed) {
                ffmpeg.stdin.write(frame);
            }
        } catch (err) {
            console.error(`[${vdsNo}] mjpeg_saved ffmpeg write error:`, err.message);
        }
    };


    /*const listener = (frame) => {
        if (ffmpeg.stdin.writable) ffmpeg.stdin.write(frame);
    };*/

    stream.listeners.push(listener);

    ffmpeg.stdin.on('error', (err) => {
        console.error(`[${vdsNo}] captureMJPEG ffmpeg.stdin error:`, err.message);
    });
    
    const cleanup = () => {

        clearTimeout(timeout);
        savingInProgressSet.delete(vdsNo);

        alreadyCleanedUp = true;

        stream.listeners = stream.listeners.filter(fn => fn !== listener);

        stream.lockedForSaving = false; //

        if (stream.clients === 0 && stream.listeners.length === 0) {

            console.log(`mjpeg_saved clients = ${stream.clients} listener length = ${stream.listeners.length}`);

            if (stream._rawRes && stream._rawRes.destroy) {
                console.log(`[${vdsNo}] mjpeg_saved res.data stream destroy 시도`);
                stream._rawRes.destroy();  //
                stream._rawRes = null;
            }

            if (stream.ffmpegrtsp) { //rtsp
                try {
                    stream.ffmpegrtsp?.stdin?.end();
                    stream.ffmpegrtsp?.kill('SIGKILL');
                } catch (e) {
                }
            }

            if (stream.ffmpeg) {
                try {
                    stream.ffmpeg?.stdin?.end();
                    stream.ffmpeg?.kill('SIGKILL');
                } catch (e) {

                }
            }

            stream.active = false;
            vdsStreamMap.delete(vdsNo);

            process.send({
                type: 'status_update',
                pid: process.pid,
                vdsList: Array.from(vdsStreamMap.keys()), //
            });

          
        }
    };

    ffmpeg.on('close', (code, signal) => {

        console.log(`[${vdsNo}] mjpeg_saved ffmpeg closed. code=${code}, signal=${signal}`);

        cleanup();

        if (code === 0) {
            const thumb = filename.replace('.mp4', '.jpg');

            const ffmpegThumb = spawn('ffmpeg', ['-y', '-i', filename, '-ss', '00:00:01', '-vframes', '1', thumb]);

            ffmpegThumb.on('close', (thumbCode) => {
                if (thumbCode === 0) {
                    process.send({ type: 'mjpeg_saved', vdsNo, filename, thumb });
                } else {
                  
                    process.send({ type: 'mjpeg_saved', vdsNo, filename, error: true });
                }
            });

            ffmpegThumb.on('error', (err) => {
                process.send({ type: 'mjpeg_saved', vdsNo, filename, error: true });
            });

        } else {

            process.send({ type: 'mjpeg_saved', vdsNo, filename, error: true });
        }
    });

    ffmpeg.on('error', (err) => {

        console.error(`[${vdsNo}]  mjpeg_saved ffmpeg process error:`, err.message);

        cleanup();

        /*clearTimeout(timeout);
        savingInProgressSet.delete(vdsNo);
        stream.listeners = stream.listeners.filter(fn => fn !== listener);*/
        process.send({ type: 'mjpeg_saved', vdsNo, filename, error: true });
    });


    ffmpeg.on('exit', (code, signal) => {
        



    });

}


function handleMJPEGWorker() {

    process.on('message', (msg) => {

        if (msg.type === 'initStatus') {

            console.log(`recv initStatus message`);

            process.send({
                type: 'status_update',
                pid: process.pid,
                vdsList: [],
            });
        }
        else if (msg.type === 'capture_mjpeg') {
            console.log(msg);

            captureMJPEG(msg.vdsNo, msg.url, msg.duration, msg.filename);
        }
        else if (msg.type === 'start_stream') {
            const vdsNo = msg.vdsNo;
            const mjpegUrl = msg.url;

            console.log(`start_stream vdsNo:${vdsNo} url:${mjpegUrl}`);

            if (!vdsStreamMap.has(vdsNo)) {

                if (mjpegUrl.startsWith('rtsp://')) {

        
                    startRtspToMjpeg(vdsNo, mjpegUrl, (errorMsg) => {
                        process.send({
                            type: 'streamFail',
                            vdsNo,
                            errorMsg
                        });

                    });

                } else {
                   

                    startMJPEGProxyStream(vdsNo, mjpegUrl, (errorMsg) => {
                        process.send({
                            type: 'streamFail',
                            vdsNo,
                            errorMsg
                        });
                        /*ws.send(JSON.stringify({
                            type: 'stream_error',
                            vdsNo,
                            message: errorMsg
                        }));*/
                    });
                }
            }

            const stream = vdsStreamMap.get(vdsNo);

            if (stream.forRecordingOnly) {
               
                attachFFmpegToStream(vdsNo, stream);

                stream.forRecordingOnly = false; //
            }

            //console.log(`${vdsNo} ws add listener size ${stream.listeners.length} clients ${stream.clients}`);

            process.send({
                type: 'status_update',
                pid: process.pid,
                vdsList: Array.from(vdsStreamMap.keys()), // 
            });
        }
        else if (msg.type === 'stop_stream') {

            const vdsNo = msg.vdsNo;

            if (vdsStreamMap.has(vdsNo)) {

                console.log(`[${vdsNo}] recv stop_stream `);

                const stream = vdsStreamMap.get(vdsNo);

                if (stream.listeners.length === 0) { //

                    try {
                        stream._rawRes?.destroy?.();
                      
                    } catch (e) {
                        
                    }

                    if (stream.ffmpeg) {
                        try {
                            stream.ffmpeg?.stdin?.end();
                            stream.ffmpeg?.kill('SIGKILL');
                        } catch (e) {
                            
                        }
                    }

                    if (stream.ffmpegrtsp) {
                        try {
                            stream.ffmpegrtsp?.stdin?.end();
                            stream.ffmpegrtsp?.kill('SIGKILL');
                        } catch (e) {
                            
                        }
                    }
                }

                vdsStreamMap.delete(vdsNo);

                process.send({
                    type: 'status_update',
                    pid: process.pid,
                    vdsList: Array.from(vdsStreamMap.keys()),
                });
            }
        }
        else if (msg.type === 'update_clients') {

            const vdsNo = msg.vdsNo;
            const stream = vdsStreamMap.get(vdsNo);

            if (stream) {
                stream.clients = msg.clientCount;

                if (stream.clients === 0 && stream.listeners.length === 0) { //

                    try {
                        stream._rawRes?.destroy?.();
                       
                    } catch (e) {

                    }

                    if (stream.ffmpeg) {
                        try {
                            stream.ffmpeg?.stdin?.end();
                            stream.ffmpeg?.kill('SIGKILL');
                        } catch (e) {
                           
                        }
                    }

                    if (stream.ffmpegrtsp) {
                        try {
                            stream.ffmpegrtsp?.stdin?.end();
                            stream.ffmpegrtsp?.kill('SIGKILL');
                        } catch (e) {
                           
                        }
                    }

                    vdsStreamMap.delete(vdsNo);

                    process.send({
                        type: 'status_update',
                        pid: process.pid,
                        vdsList: Array.from(vdsStreamMap.keys()),
                    });
                }
            } else {
               
            }
        }
    });

    process.on('SIGINT', () => {
        
        gracefulShutdown('SIGINT')
    });

    process.on('SIGTERM', () => {
        gracefulShutdown('SIGTERM')
    });
}



function cleanupHLS(vdsNo) {
    return new Promise((resolve) => {
        if (!fs.existsSync(HLS_DIR)) return resolve();

        fs.readdir(HLS_DIR, (err, files) => {
            if (err) {
              
                return resolve();
            }

            const prefix = `vds${vdsNo}`; // vds${vdsNo}.m3u8 vds${vdsNo}%04d.ts
            let pending = 0;

            files.forEach((file) => {
                if (file.startsWith(prefix)) {
                    const filePath = path.join(HLS_DIR, file);
                    pending++;
                    fs.unlink(filePath, (err) => {
                        if (err && err.code !== 'ENOENT') {  //
                           
                        } else {
                    
                        }
                        pending--;
                        if (pending === 0) resolve();
                    });
                }
            });

            if (pending === 0) resolve();
        });
    });
}




const gracefulShutdown = async (signal) => {
    logger.error(`MJPEG Worker ${signal} received. Starting shutdown...`);

    for (const [vdsNo, streamInfo] of vdsStreamMap.entries()) {

        await cleanupHLS(vdsNo);

        streamInfo.active = false;
        streamInfo.listeners = [];

        /*streamInfo.clients.forEach(ws => {
            if (ws.readyState === WebSocket.OPEN) ws.close();
        });
        streamInfo.clients.clear();*/

        if (streamInfo._rawRes?.destroy) {
            try {
                streamInfo._rawRes.destroy();
                console.log(`[${vdsNo}] _rawRes destroy`);
            } catch (e) { }
        }

        if (streamInfo.ffmpeg) {
            try {
                streamInfo.ffmpeg.stdin?.end();
                streamInfo.ffmpeg.kill('SIGKILL');
            } catch (e) { }
        }

        if (streamInfo.ffmpegrtsp) {
            try {
                streamInfo.ffmpegrtsp?.stdin?.end();
                streamInfo.ffmpegrtsp?.kill('SIGKILL');
            } catch (e) {
            }
        }

        vdsStreamMap.delete(vdsNo);
    }

};


module.exports = { handleMJPEGWorker };













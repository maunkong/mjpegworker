// mjpeg-ws-server.js
const axios = require('axios');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

const logger = require('./logger');

const HLS_DIR = path.join(__dirname, 'hls');

const savingInProgressSet = new Set();
const vdsStreamMap = new Map();

function insertExtXStartTag(vdsNo) { //덮어쓰기 때문에 의미 없음
    const outputFile = path.join(HLS_DIR, `vds${vdsNo}.m3u8`);

    let retryCount = 0;
    const MAX_RETRY = 20;

    const tryInsert = () => {
        if (!fs.existsSync(outputFile)) {
            if (++retryCount > MAX_RETRY) {
                console.warn(`[${vdsNo}] #EXT-X-START 삽입 실패: 파일 생성 안 됨 ${retryCount} ${MAX_RETRY}`);
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
                    else console.log(`[${vdsNo}] #EXT-X-START 삽입 완료`);
                });
            }
        });
    };

    tryInsert();
}


function attachFFmpegToStream(vdsNo, stream) {

    if (stream.ffmpeg) {
        console.warn(`[${vdsNo}] ffmpeg 이미 있음, attach 중단`);
        return;
    }

    if (stream.type === 'rtsp') {
        if (!stream._stdout || stream._stdout.readableEnded || stream._stdout.destroyed) {
            console.error(`[${vdsNo}] 원본 RTSP 스트림 종료됨. ffmpeg attach 불가`);
            return;
        }
    }
    else { //mjpeg
        if (!stream._rawRes || stream._rawRes.readableEnded || stream._rawRes.destroyed) {
            console.error(`[${vdsNo}] 원본 MJPEG 스트림 종료됨. ffmpeg attach 불가`);
            return;
        }
    }

    const outputFile = path.join(HLS_DIR, `vds${vdsNo}.m3u8`);
    const outputtsFile = path.join(HLS_DIR, `vds${vdsNo}%04d.ts`);

    const ffmpeg = spawn('ffmpeg', [
        '-f', 'mjpeg',
        '-i', 'pipe:0',
        '-vf', 'scale=640:360:force_original_aspect_ratio=decrease', // url 나 pipe 다음에 써야함 426:240
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
        console.log(`[${vdsNo}] ffmpeg 종료됨 (attach). code=${code}, signal=${signal}`);

        if (code === null && signal === 'SIGKILL') {
            console.error(`[${vdsNo}] attached streaming ffmpeg가 강제 종료됨. 원인: 스트림 없음 or 시스템 종료`);
        }
    });

    ffmpeg.on('error', (err) => {
        console.error(`[${vdsNo}] streaming ffmpeg attach 실패:`, err.message);
    });

    stream.ffmpeg = ffmpeg;

    console.log(`[${vdsNo}] streaming ffmpeg attach 완료`);

}


function startMJPEGProxyStream(vdsNo, mjpegUrl, onFail, { forRecordingOnly = false } = {}) {

    const existing = vdsStreamMap.get(vdsNo);

    if (existing) {

        const hasClients = existing.clients > 0;
        const hasListeners = existing.listeners && existing.listeners.length > 0;

        if (existing.active && (hasClients || hasListeners)) {
            console.warn(`[${vdsNo}] 스트림이 사용 중이므로 재시작하지 않음`);
            return; //저장 중이거나 시청 중이면 그냥 유지
        }

        console.log(`[${vdsNo}] 기존 MJPEG 스트림 정리 후 재시작`);
        existing.active = false;
        existing.listeners = [];

        try {
            existing._rawRes?.destroy?.();
            console.log(`[${vdsNo}] 기존 스트림 MJPEG _rawRes destroy`);
        } catch (e) {
            console.warn(`[${vdsNo}] 기존 스트림 _rawRes destroy 실패:`, e.message);
        }

        if (/*!forRecordingOnly &&*/ existing.ffmpeg ) {
            try {
                existing.ffmpeg?.stdin?.end();
                existing.ffmpeg?.kill('SIGKILL');
            } catch (e) {
                console.error(`[${vdsNo}] 기존 ffmpeg 종료 중 오류:`, e.message);
            }
        }

        if ( existing.ffmpegrtsp) {
            try {
                existing.ffmpegrtsp?.stdin?.end();
                existing.ffmpegrtsp?.kill('SIGKILL');
            } catch (e) {
                console.error(`[${vdsNo}] 기존 ffmpegrtsp 종료 중 오류:`, e.message);
            }
        }

        vdsStreamMap.delete(vdsNo); //이상 삭제 후 재추가
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
        forRecordingOnly //명시적으로 저장
    };

    console.log(`req vds:${vdsNo} stream current length ${vdsStreamMap.size}`);

    let ffmpeg = null;

    if (!forRecordingOnly) { //스트림일 경우
        const outputFile = path.join(HLS_DIR, `vds${vdsNo}.m3u8`);
        const outputtsFile = path.join(HLS_DIR, `vds${vdsNo}%04d.ts`);

        //pipe: 0	stdin(표준 입력)에서 데이터를 읽음
        //pipe: 1	stdout(표준 출력)으로 데이터를 씀
        //pipe: 2	stderr(표준 에러 출력)으로 씀
        //ffmpeg로 HLS 스트림 변환 시작
        /*이름	해상도
        144p	256×144
        240p	426×240 
        360p	640×360
        480p	854×480
        720p(HD)	1280×720
        1080p(FHD)	1920×1080*/

        ffmpeg = spawn('ffmpeg', [
            '-f', 'mjpeg',
            //'-r', '30', // 초당 15frame
            //'-vf', 'scale=1280:720:force_original_aspect_ratio=decrease', //720P 강제변환
            '-i', 'pipe:0', //'-i', 'pipe:0' → stdin을 입력 소스로 사용한다.  입력이 RTSP일 경우에는 pipe:0 방식 안됨
            '-vf', 'scale=640:360:force_original_aspect_ratio=decrease', // url 나 pipe 다음에 써야함 426:240
            '-c:v', 'libx264',
            //'-t', '300', // 5분만
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
            console.error(`[${vdsNo}] ffmpeg.stdin 에러:`, err.message);

            if (typeof onFail === 'function') {
                console.error(`[${vdsNo}] ffmpeg.stdin 에러:`, err.message);
                onFail(`stdin error: ${err.message}`);
            }

            cleanup();
        });

        ffmpeg.on('exit', (code, signal) => {
            console.log(`[${vdsNo}] ffmpeg exited. code=${code}, signal=${signal}`);

            if (code === null && signal === 'SIGKILL') {
                console.error(`[${vdsNo}]  ffmpeg가 강제 종료됨. 원인: 스트림 없음 or 시스템 종료`);
            }

            cleanup();
        });

        ffmpeg.on('error', (err) => {
            console.error(`[${vdsNo}] ffmpeg 실행 오류`, err);

            if (typeof onFail === 'function') {
                console.error(`[${vdsNo}] ffmpeg 실행 오류`, err);
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
            console.warn(`[${vdsNo}] 저장 중이므로 cleanup 생략`);
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

        console.log(`[${vdsNo}] cleanup 완료`);

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

            console.log(`[${vdsNo}] cleanup 완료 및 파일 삭제`);
        }
    };

    axios({
        url: mjpegUrl,
        responseType: 'stream',
        headers: { 'User-Agent': 'Node.js' },
    }).then((res) => {

        streamInfo._rawRes = res.data; // 여기에 저장

        const MAX_BUFFER_SIZE = 1 * 1024 * 1024;

        let lastFrameTime = Date.now();
        const INACTIVITY_TIMEOUT = 15000; //15초동안 안 들어오면 강제종료

        inactivityTimer = setInterval(() => {
            if (Date.now() - lastFrameTime > INACTIVITY_TIMEOUT) {
                console.warn(`[${vdsNo}] 프레임 수신 없음. 스트림 강제 종료.`);

                if (typeof onFail === 'function') {
                    console.warn(`[${vdsNo}] 프레임 수신 없음. 스트림 강제 종료.`);
                    onFail(`[${vdsNo}] 프레임 수신 없음. 스트림 강제 종료.`);
                }

                //if (res.data && res.data.destroy) res.data.destroy();
                //clearInterval(inactivityTimer);
                streamInfo.lockedForSaving = false; //저장도 취소
                cleanup(); // 이 시점에 clearInterval도 동작함
            }
        }, 1000);

        let insertedExtXStart = false;

        let lastCleanupTime = 0; // 마지막 삭제 시각
        let endTimeout = null;

        const CLEANUP_INTERVAL = 1000; // 최소 1초 간격
        const MAX_TS_FILES = 100;
        const KEEP_TS_COUNT = 50;

        res.data.on('data', (chunk) => {

            //console.log(`[${vdsNo}] chunk ${chunk.length} bytes`);
            if (!streamInfo.active) {
                //res.data.destroy?.();  //axios 연결 종료

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
                    console.log(`[${vdsNo}] 'end' 후 프레임 재수신. 종료 대기 취소`);
                }

                try {
                    if (!streamInfo.forRecordingOnly && streamInfo.ffmpeg?.stdin?.writable) {
                        streamInfo.ffmpeg.stdin.write(jpg); //HLS 전달
                    }
                } catch (e) {
                    console.error(`[${vdsNo}] ffmpeg.stdin.write 실패:`, e.message);
                    cleanup();
                }

                try {
                    streamInfo.listeners.forEach(cb => cb(jpg));  //저장 ffmjpeg 전달
                } catch (e) {
                    console.error(`[${vdsNo}] listeners error:`, e.message);
                }

                //console.log('resolution', getJpegResolution(jpg));

                /*if (!insertedExtXStart && !streamInfo.forRecordingOnly) {
                    insertedExtXStart = true;
                    insertExtXStartTag(vdsNo);
                }*/

                //ts 파일 삭제
                const now = Date.now();

                if (!streamInfo.forRecordingOnly && now - lastCleanupTime > CLEANUP_INTERVAL) {
                    lastCleanupTime = now;

                    const tsPattern = new RegExp(`^vds${vdsNo}\\d+\\.ts$`);
                    fs.readdir(HLS_DIR, (err, files) => {
                        if (err) return;
                        const tsFiles = files
                            .filter(f => tsPattern.test(f))
                            .sort(); // 이름 순으로 정렬 (vds10000.ts ~ vds10100.ts)

                        if (tsFiles.length > MAX_TS_FILES) {
                            const filesToDelete = tsFiles.slice(0, tsFiles.length - KEEP_TS_COUNT);
                            filesToDelete.forEach(file => {
                                fs.unlink(path.join(HLS_DIR, file), () => { });
                            });
                            console.log(`[${vdsNo}] ts 파일 ${filesToDelete.length}개 삭제됨`);
                        }
                    });
                }
            }
        });

        //end	데이터가 다 읽혔을 때	정상 종료, 스트림 내용 소진	보통 유예 후 cleanup
        //close	스트림이 완전히 닫힘	리소스 해제 완료	 cleanup 
        //aborted	클라이언트가 중간에 끊음	비정상 종료	 cleanup 
        //error	스트림에서 에러 발생	예외 상황	 cleanup or 재시도
        //res.data는 ReadableStream인데, 일반적으로:

        //end 이벤트는 스트림이 더 이상 데이터를 전달하지 않을 때 호출됨
        //근데 현실에서는 아래와 같은 상황이 예외적으로 생김:
        //장비(MJPEG / RTSP)가 스트림을 잠시 멈췄다가 재개
        //네트워크 지연 / 버퍼링 / WiFi 불안정 / RTSP 컨트롤 흐름 등
        //axios 내부 keep - alive / 연결 재시도에 의해 재접속이 감지되지 않음
        //end는 받았지만 소켓은 살아있거나, 곧바로 reconnect함
        //프록시 / 리버스 프록시 레벨에서 일시적인 커넥션 리셋 후 재데이터 수신
        //ffmpeg가 stdin 쪽 버퍼가 가득 차서 잠깐 멈췄다가 다시 풀리는 현상


        res.data.on('aborted', () => {
            console.warn(`[${vdsNo}] MJPEG stream aborted by client.`);
            cleanup();
        });

        res.data.on('end', () => {
            console.log(`[${vdsNo}] MJPEG stream ended.`);

            endTimeout = setTimeout(() => {
                if (Date.now() - streamInfo.lastFrameAt > 5000) {
                    console.warn(`[${vdsNo}] 'end' 이후 5초간 프레임 없음 cleanup 실행`);
                    cleanup();
                } else {
                    console.log(`[${vdsNo}] 'end' 후 프레임 재수신 cleanup 취소`);
                }
            }, 5000);
        });

        res.data.on('error', (err) => {
            console.error(`[${vdsNo}] MJPEG stream error:`, err.message);
            cleanup();
        });

        res.data.on('close', () => {  //close: TCP 닫힘
            console.log(`[${vdsNo}] res.data stream closed`);
            cleanup();
        });

    }).catch((err) => {
        //console.error(`[${vdsNo}] MJPEG stream failed:`, err.message);

        if (typeof onFail === 'function') {
            console.error(`[${vdsNo}] MJPEG stream failed:`, err.message);

            onFail(err.message);
        }

        streamInfo.lockedForSaving = false; //저장도 취소

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
            console.warn(`[${vdsNo}] 스트림이 사용 중이므로 재시작하지 않음`);
            return; //저장 중이거나 시청 중이면 그냥 유지
        }

        console.log(`[${vdsNo}] 기존 스트림 정리 후 재시작`);
        existing.active = false;
        existing.listeners = [];

        try {
            existing._rawRes?.destroy?.();
            console.log(`[${vdsNo}] 기존 스트림 _rawRes destroy`);
        } catch (e) {
            console.warn(`[${vdsNo}] 기존 스트림 _rawRes destroy 실패:`, e.message);
        }

        if (/*!forRecordingOnly &&*/ existing.ffmpeg) {
            try {
                existing.ffmpeg?.stdin?.end();
                existing.ffmpeg?.kill('SIGKILL');
            } catch (e) {
                console.error(`[${vdsNo}] 기존 ffmpeg 종료 중 오류:`, e.message);
            }
        }

        if (existing.ffmpegrtsp) {
            try {
                existing.ffmpegrtsp?.stdin?.end();
                existing.ffmpegrtsp?.kill('SIGKILL');
            } catch (e) {
                console.error(`[${vdsNo}] 기존 ffmpegrtsp 종료 중 오류:`, e.message);
            }
        }

        vdsStreamMap.delete(vdsNo); //이상 삭제 후 재추가
    }

    const streamInfo = {
        type: 'rtsp',
        ffmpeg: null,
        ffmpegrtsp: null,
        buffer: Buffer.alloc(0),
        clients: 0,
        listeners: [],
        active: true,
        _stdout: null,          // ffmpeg stdout
        _rawRes: null,          // axios 스트림
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
        ffmpeg.on('exit', () => console.log(`[${vdsNo}] RTSP ffmpeg 종료됨`));

        streamInfo.ffmpeg = ffmpeg;
    }

    streamInfo.ffmpegrtsp = ffmpegrtsp;
    streamInfo._stdout = ffmpegrtsp.stdout;

    let lastFrameTime = Date.now();
    const INACTIVITY_TIMEOUT = 15000; //rtsp 느림 20초

    const MAX_BUFFER_SIZE = 1 * 1024 * 1024;

    const inactivityTimer = setInterval(() => {
        if (Date.now() - lastFrameTime > INACTIVITY_TIMEOUT) {

            if (typeof onFail === 'function') {
                console.warn(`[${vdsNo}] 프레임 수신 없음. 스트림 강제 종료.`);
                onFail(`[${vdsNo}] 프레임 수신 없음. 스트림 강제 종료.`);
            }

            streamInfo.lockedForSaving = false;

            cleanup();
        }

    }, 1000);


    let alreadyCleanedUp = false;

    const cleanup = () => {
        if (alreadyCleanedUp) return;

        if (streamInfo.lockedForSaving) {
            console.warn(`[${vdsNo}] rtsp 저장 중이므로 cleanup 생략`);
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

        console.log(`[${vdsNo}] RTSP cleanup 완료`);

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

            console.log(`[${vdsNo}] cleanup 완료 및 파일 삭제`);
        }

    };


    ffmpegrtsp.stderr.on('data', () => { });

    ffmpegrtsp.on('exit', () => {
        console.log(`[${vdsNo}] ffmpegrtsp 종료됨`);
        cleanup();
    });

    ffmpegrtsp.on('error', (err) => {
        console.error(`[${vdsNo}] ffmpegrtsp 오류`, err.message);

        if (typeof onFail === 'function') {
            console.warn(`[${vdsNo}]  ffmpegrtsp 오류.`);
            onFail(`[${vdsNo}] ffmpegrtsp 오류.`, err.message);
        }

        cleanup();
    });

    ffmpegrtsp.stdin.on('error', err => {

        console.error(`[${vdsNo}] ffmpeg.stdin error`, err.message);

        cleanup();
    });

    ffmpegrtsp.on('exit', () => {

        console.log(`[${vdsNo}] ffmpegHLS 종료됨`);
        cleanup();
    });

    let lastCleanupTime = 0; // 마지막 삭제 시각
    const CLEANUP_INTERVAL = 1000; // 최소 1초 간격
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
                    console.error(`[${vdsNo}] ffmpeg.stdin.write 실패:`, e.message);

                    streamInfo.lockedForSaving = false; //에러났으니 false
                    cleanup();
                }
            }

            try {
                streamInfo.listeners.forEach(cb => cb(jpg));  //저장 ffmjpeg 전달
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
                        .sort(); // 이름 순으로 정렬 (vds10000.ts ~ vds10100.ts)

                    if (tsFiles.length > MAX_TS_FILES) {
                        const filesToDelete = tsFiles.slice(0, tsFiles.length - KEEP_TS_COUNT);
                        filesToDelete.forEach(file => {
                            fs.unlink(path.join(HLS_DIR, file), () => { });
                        });
                        console.log(`[${vdsNo}] ts 파일 ${filesToDelete.length}개 삭제됨`);
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
        console.warn(`[${vdsNo}] 중계 스트림 없음. 자동 시작 시도. ${stream} `);

        if (mjpegUrl.startsWith('rtsp://')) {

            console.log(`[${vdsNo}] RTSP 스트림 시작`);

            startRtspToMjpeg(vdsNo, mjpegUrl, (errMsg) => {
                console.error(`[${vdsNo}] 스트림 연결 실패: ${errMsg}`);
                process.send({ type: 'mjpeg_saved', vdsNo, filename, error: true, message: errMsg });
            }, { forRecordingOnly: true }); //저장 일때만 true

            if (retryCount >= 3) {
                console.error(`[${vdsNo}] 스트림 재시도 3회 초과. 저장 실패.`);
                process.send({ type: 'mjpeg_saved', vdsNo, filename, error: true });
                return;
            }

            setTimeout(() => {
                console.error(`[${vdsNo}] 연결 재시도: retrycount ${retryCount}`);
                captureMJPEG(vdsNo, mjpegUrl, duration, filename, retryCount + 1);
            }, 1000); // 1초 대기 후 재시도

        } else {
            console.log(`[${vdsNo}] MJPEG 스트림 시작`);

            startMJPEGProxyStream(vdsNo, mjpegUrl, (errMsg) => {
                console.error(`[${vdsNo}] 스트림 연결 실패: ${errMsg}`);
                process.send({ type: 'mjpeg_saved', vdsNo, filename, error: true, message: errMsg });
            }, { forRecordingOnly: true }); //저장 일때만 true

            if (retryCount >= 3) {
                console.error(`[${vdsNo}] 스트림 재시도 3회 초과. 저장 실패.`);
                process.send({ type: 'mjpeg_saved', vdsNo, filename, error: true });
                return;
            }

            setTimeout(() => {
                console.error(`[${vdsNo}] 연결 재시도: retrycount ${retryCount}`);
                captureMJPEG(vdsNo, mjpegUrl, duration, filename, retryCount + 1);
            }, 1000); // 1초 대기 후 재시도
        }

        return;
    }

    process.send({
        type: 'status_update',
        pid: process.pid,
        vdsList: Array.from(vdsStreamMap.keys()), // 그냥 keys만
    });

    stream.lockedForSaving = true;   //  저장 중 cleanup 방지
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

    const timeoutMs = (duration * 2) * 1000; //여유 있게
    const timeout = setTimeout(() => {

        console.error(`[${vdsNo}] mjpeg_saved ffmpeg timeout 강제종료 ms${timeoutMs}`);

        try {
            ffmpeg.stdin?.end(); // 버퍼 플러시 유도
            ffmpeg.kill('SIGKILL');
        } catch (e) {
            console.error(`[${vdsNo}] ffmpeg kill error:`, e.message);
        }

    }, timeoutMs); //종료 안될경우 강제 KILL

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

    // 반드시 먼저 제거 (listener 살아 있으면 write 시도함)
    const cleanup = () => {

        clearTimeout(timeout);
        savingInProgressSet.delete(vdsNo);

        alreadyCleanedUp = true;

        stream.listeners = stream.listeners.filter(fn => fn !== listener);

        stream.lockedForSaving = false; //  저장 종료 후 해제

        if (stream.clients === 0 && stream.listeners.length === 0) {

            console.log(`mjpeg_saved clients = ${stream.clients} listener length = ${stream.listeners.length}`);

            if (stream._rawRes && stream._rawRes.destroy) {
                console.log(`[${vdsNo}] mjpeg_saved res.data stream destroy 시도`);
                stream._rawRes.destroy();  //axios로부터 받은 MJPEG 응답 스트림(res.data)를 강제로 종료
                stream._rawRes = null;
            }

            if (stream.ffmpegrtsp) { //rtsp
                try {
                    stream.ffmpegrtsp?.stdin?.end();
                    stream.ffmpegrtsp?.kill('SIGKILL');
                } catch (e) {
                }
                /*finally {
                    stream.ffmpegrtsp = null; // KILL해도 null 안됨 명시적선언필요 
                }*/
            }

            if (stream.ffmpeg) {
                try {
                    stream.ffmpeg?.stdin?.end();
                    stream.ffmpeg?.kill('SIGKILL');
                } catch (e) {
                    console.error(`[${vdsNo}] 기존 ffmpeg 종료 중 오류:`, e.message);
                }
            }

            stream.active = false;
            vdsStreamMap.delete(vdsNo);

            process.send({
                type: 'status_update',
                pid: process.pid,
                vdsList: Array.from(vdsStreamMap.keys()), // 그냥 keys만
            });

            console.log(`[${vdsNo}] 저장/시청 모두 종료됨. 스트림 정리 완료`);
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
                    logger.error(`[${vdsNo}] 썸네일 생성 실패 (code=${thumbCode})`);
                    console.error(`[${vdsNo}] 썸네일 생성 실패 (code=${thumbCode})`);
                    process.send({ type: 'mjpeg_saved', vdsNo, filename, error: true });
                }
            });

            ffmpegThumb.on('error', (err) => {
                logger.error(`[${vdsNo}] 썸네일 ffmpeg 프로세스 오류:`, err.message);
                process.send({ type: 'mjpeg_saved', vdsNo, filename, error: true });
            });

        } else {

            logger.error(`[${vdsNo}] mp4 저장 실패 (code=${code})`);

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
        console.log(`[${vdsNo}] mjpeg_saved ffmpeg 종료됨. code=${code}, signal=${signal}`);

        if (code === null && signal === 'SIGKILL') {
            console.error(`[${vdsNo}] mjpeg_saved ffmpeg가 강제 종료됨. 원인: 스트림 없음 or 시스템 종료`);
        }

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

                    console.log(`[${vdsNo}] RTSP 스트림 시작`);

                    startRtspToMjpeg(vdsNo, mjpegUrl, (errorMsg) => {
                        process.send({
                            type: 'streamFail',
                            vdsNo,
                            errorMsg
                        });

                    });

                } else {
                    console.log(`[${vdsNo}] MJPEG 스트림 시작`);

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
                console.log(`[${vdsNo}] forRecordingOnly 스트림에 ffmpeg attach`);
                attachFFmpegToStream(vdsNo, stream);

                stream.forRecordingOnly = false; //변경
            }

            //console.log(`${vdsNo} ws add listener size ${stream.listeners.length} clients ${stream.clients}`);

            process.send({
                type: 'status_update',
                pid: process.pid,
                vdsList: Array.from(vdsStreamMap.keys()), // 그냥 keys만
            });
        }
        else if (msg.type === 'stop_stream') {

            const vdsNo = msg.vdsNo;

            if (vdsStreamMap.has(vdsNo)) {

                console.log(`[${vdsNo}] recv stop_stream `);

                const stream = vdsStreamMap.get(vdsNo);

                if (stream.listeners.length === 0) { //저장중 아닐때만

                    try {
                        stream._rawRes?.destroy?.();
                        console.log(`[${vdsNo}] 기존 스트림 _rawRes destroy`);
                    } catch (e) {
                        console.warn(`[${vdsNo}] 기존 스트림 _rawRes destroy 실패:`, e.message);
                    }

                    if (stream.ffmpeg) {
                        try {
                            stream.ffmpeg?.stdin?.end();
                            stream.ffmpeg?.kill('SIGKILL');
                        } catch (e) {
                            console.error(`[${vdsNo}] 기존 ffmpeg 종료 중 오류:`, e.message);
                        }
                    }

                    if (stream.ffmpegrtsp) {
                        try {
                            stream.ffmpegrtsp?.stdin?.end();
                            stream.ffmpegrtsp?.kill('SIGKILL');
                        } catch (e) {
                            console.error(`[${vdsNo}] 기존 ffmpegrtsp 종료 중 오류:`, e.message);
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

                if (stream.clients === 0 && stream.listeners.length === 0) { //혹시 모르니 체크

                    console.log(`[${vdsNo}] 워커: 시청자 0 저장 0 스트림 종료`);

                    try {
                        stream._rawRes?.destroy?.();
                        console.log(`[${vdsNo}] 기존 스트림 _rawRes destroy`);
                    } catch (e) {
                        console.warn(`[${vdsNo}] 기존 스트림 _rawRes destroy 실패:`, e.message);
                    }

                    if (stream.ffmpeg) {
                        try {
                            stream.ffmpeg?.stdin?.end();
                            stream.ffmpeg?.kill('SIGKILL');
                        } catch (e) {
                            console.error(`[${vdsNo}] 기존 ffmpeg 종료 중 오류:`, e.message);
                        }
                    }

                    if (stream.ffmpegrtsp) {
                        try {
                            stream.ffmpegrtsp?.stdin?.end();
                            stream.ffmpegrtsp?.kill('SIGKILL');
                        } catch (e) {
                            console.error(`[${vdsNo}] 기존 ffmpegrtsp 종료 중 오류:`, e.message);
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
                console.warn(`[${msg.vdsNo}] 워커: update_clients 받았지만 stream 없음`);
            }
        }
    });

    process.on('SIGINT', () => {
        console.log('[MJPEG 워커] SIGINT 수신됨!');
        gracefulShutdown('SIGINT')
    });

    process.on('SIGTERM', () => {
        console.log('[MJPEG 워커] SIGTERM 수신됨!');
        gracefulShutdown('SIGTERM')
    });
}



function cleanupHLS(vdsNo) {
    return new Promise((resolve) => {
        if (!fs.existsSync(HLS_DIR)) return resolve();

        fs.readdir(HLS_DIR, (err, files) => {
            if (err) {
                console.error('[HLS Cleanup] 디렉터리 읽기 실패:', err);
                return resolve();
            }

            const prefix = `vds${vdsNo}`; // vds${vdsNo}.m3u8 vds${vdsNo}%04d.ts
            let pending = 0;

            files.forEach((file) => {
                if (file.startsWith(prefix)) {
                    const filePath = path.join(HLS_DIR, file);
                    pending++;
                    fs.unlink(filePath, (err) => {
                        if (err && err.code !== 'ENOENT') {  //ENOENT는 정상적인 경쟁 상황에서 자연스럽게 발생하는 에러
                            console.error(`[HLS Cleanup] 파일 삭제 실패: ${filePath}`, err.message);
                        } else {
                            console.log(`[HLS Cleanup] 삭제됨: ${filePath}`);
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

    // 스트림 정리
    for (const [vdsNo, streamInfo] of vdsStreamMap.entries()) {

        await cleanupHLS(vdsNo); // 삭제 완료

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


/*if (!stream || !stream.active) {
    console.warn(`[${vdsNo}] 중계 스트림 없음. 새로 시작 시도`);

    try {
        stream = await new Promise((resolve, reject) => {
            startMJPEGProxyStream(vdsNo, (errMsg) => {
                reject(`[${vdsNo}] MJPEG 연결 실패: ${errMsg}`);
            }, { forRecordingOnly: true });

            // 연결 대기용 체크 (최대 3초)
            const timeout = setTimeout(() => reject(`[${vdsNo}] MJPEG 연결 시간 초과`), 3000);

            const checkInterval = setInterval(() => {
                const newStream = vdsStreamMap.get(vdsNo);
                if (newStream && newStream.active) {
                    clearInterval(checkInterval);
                    clearTimeout(timeout);
                    resolve(newStream);
                }
            }, 300);
        });
    } catch (err) {
        console.error(err);
        process.send({ type: 'mjpeg_saved', vdsNo, filename, error: true, message: err });
        return;
    }
}*/



/*function getJpegResolution(jpgBuffer) { //해상도 가져오기 함수
    let offset = 2; // 0xffd8 이후부터 검사

    while (offset < jpgBuffer.length) {
        if (jpgBuffer[offset] !== 0xFF) {
            break;
        }

        const marker = jpgBuffer[offset + 1];

        // SOF0 ~ SOF2 : 해상도 정보 포함됨
        if (marker >= 0xC0 && marker <= 0xC2) {
            const height = jpgBuffer.readUInt16BE(offset + 5);
            const width = jpgBuffer.readUInt16BE(offset + 7);
            return { width, height };
        }

        const length = jpgBuffer.readUInt16BE(offset + 2);
        offset += 2 + length;
    }

    return null;
}

const INSERT_TIMEOUT = 5000; // 최대 대기시간
const CHECK_INTERVAL = 1000; // 1초마다 확인*/

/*const fsPromises = require('fs').promises;

async function deleteHLSFiles(vdsNo) {
    const m3u8File = path.join(HLS_DIR, `vds${vdsNo}.m3u8`);
    const tsPattern = new RegExp(`^vds${vdsNo}\\d+\\.ts$`);

    try {
        await fsPromises.unlink(m3u8File);
        console.log(`[${vdsNo}] m3u8 삭제 완료`);
    } catch (err) {
        if (err.code !== 'ENOENT') console.error(`[${vdsNo}] m3u8 삭제 실패:`, err.message);
    }

    try {
        const files = await fsPromises.readdir(HLS_DIR);
        const matched = files.filter(f => tsPattern.test(f));

        await Promise.all(matched.map(async (f) => {
            try {
                await fsPromises.unlink(path.join(HLS_DIR, f));
                console.log(`[${vdsNo}] ts 삭제 완료: ${f}`);
            } catch (err) {
                console.error(`[${vdsNo}] ts 삭제 실패: ${f} - ${err.message}`);
            }
        }));
    } catch (err) {
        console.error(`[${vdsNo}] ts 목록 조회 실패:`, err.message);
    }
}


function stopUnusedStreams(validRange = []) {
    for (const [vdsNo, stream] of vdsStreamMap.entries()) {
        if (!validRange.includes(Number(vdsNo))) {
            if (stream.ffmpeg) stream.ffmpeg.kill('SIGKILL');


            //stream.clients?.forEach(ws => ws.close());
            vdsStreamMap.delete(vdsNo);
            console.log(`[${vdsNo}] 정리 완료`);
        }
    }
}*/













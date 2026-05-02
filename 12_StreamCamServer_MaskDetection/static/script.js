document.addEventListener('DOMContentLoaded', () => {
    var host = window.location.host;
    var socket = io(`wss://${host}`);
    var video = document.getElementById('video');

    // ── Create overlay canvas (sits on top of video to show annotated result) ──
    var overlay = document.createElement('canvas');
    overlay.id = 'overlay';
    overlay.style.position = 'absolute';
    overlay.style.top = '0';
    overlay.style.left = '0';
    overlay.style.pointerEvents = 'none';   // clicks pass through to video
    overlay.style.zIndex = '10';

    // Wrap video in a relative-positioned container if not already
    var container = video.parentElement;
    if (getComputedStyle(container).position === 'static') {
        container.style.position = 'relative';
    }
    container.appendChild(overlay);

    // ── Detection info panel ───────────────────────────────────────────────────
    var infoPanel = document.createElement('div');
    infoPanel.id = 'detection-info';
    infoPanel.style.cssText = `
        position: absolute;
        bottom: 8px;
        left: 8px;
        background: rgba(0,0,0,0.65);
        color: #fff;
        font: 13px/1.5 monospace;
        padding: 6px 10px;
        border-radius: 6px;
        z-index: 20;
        pointer-events: none;
        max-width: calc(100% - 20px);
    `;
    infoPanel.textContent = 'Waiting for detections…';
    container.appendChild(infoPanel);

    // ── Status badge ───────────────────────────────────────────────────────────
    var statusBadge = document.createElement('div');
    statusBadge.id = 'status-badge';
    statusBadge.style.cssText = `
        position: absolute;
        top: 8px;
        right: 8px;
        background: rgba(0,0,0,0.55);
        color: #facc15;
        font: bold 12px monospace;
        padding: 4px 8px;
        border-radius: 4px;
        z-index: 20;
        pointer-events: none;
    `;
    statusBadge.textContent = '⏳ Connecting…';
    container.appendChild(statusBadge);

    // ── Overlay context (lazy-sized once video metadata loads) ─────────────────
    var overlayCtx = overlay.getContext('2d');
    var captureCanvas = document.createElement('canvas');
    var captureCtx    = captureCanvas.getContext('2d');

    var frameIntervalId = null;
    var awaitingReply   = false;    // simple back-pressure: don't pile up requests
    var frameIndex      = 0;       // for saved-frame naming

    function initCanvasSizes() {
        var w = video.videoWidth  || video.width  || 640;
        var h = video.videoHeight || video.height || 480;

        captureCanvas.width  = w;
        captureCanvas.height = h;
        overlay.width        = w;
        overlay.height       = h;

        // Match overlay display size to actual video element
        overlay.style.width  = video.offsetWidth  + 'px';
        overlay.style.height = video.offsetHeight + 'px';
    }

    // ── Camera setup ───────────────────────────────────────────────────────────
    navigator.mediaDevices.getUserMedia({ video: true })
        .then(function (stream) {
            video.srcObject = stream;
            video.play();

            video.addEventListener('loadedmetadata', function () {
                initCanvasSizes();
                window.addEventListener('resize', initCanvasSizes);
            });

            // Send a frame; skip if previous reply hasn't arrived yet
            function sendFrame() {
                if (awaitingReply) return;

                if (captureCanvas.width === 0) initCanvasSizes();

                captureCtx.drawImage(video, 0, 0, captureCanvas.width, captureCanvas.height);
                var frame = captureCanvas.toDataURL('image/jpeg', 0.85);
                awaitingReply = true;
                statusBadge.textContent = '📡 Processing…';
                socket.emit('video_frame', frame);
            }

            // ~5 fps — adjust to taste
            frameIntervalId = setInterval(sendFrame, 200);
        })
        .catch(function (err) {
            console.error('Error accessing camera:', err);
            statusBadge.style.color = '#f87171';
            statusBadge.textContent = '❌ Camera error';
        });

    // ── Receive annotated frame from server ────────────────────────────────────
    socket.on('annotated_frame', function (data) {
        awaitingReply = false;
        frameIndex++;

        var img   = new Image();
        img.onload = function () {
            // Draw the annotated frame (bounding boxes + labels already burned in by OpenCV)
            overlayCtx.clearRect(0, 0, overlay.width, overlay.height);
            overlayCtx.drawImage(img, 0, 0, overlay.width, overlay.height);

            // ── Update info panel ────────────────────────────────────────────
            var detections = data.detections || [];
            if (detections.length === 0) {
                infoPanel.innerHTML = '🔍 No faces detected';
            } else {
                var lines = detections.map(function (d, i) {
                    var icon  = d.label === 'with_mask' ? '✅' : '🚫';
                    var label = d.label === 'with_mask' ? 'MASK' : 'NO MASK';
                    var pct   = (d.confidence * 100).toFixed(1) + '%';
                    var box   = d.box;
                    return `${icon} Face ${i + 1}: <b>${label}</b> — ${pct} conf &nbsp;|&nbsp; box [${box.x},${box.y} ${box.w}×${box.h}]`;
                });
                infoPanel.innerHTML = lines.join('<br>');
            }

            statusBadge.textContent = `🟢 Frame #${frameIndex}`;
            statusBadge.style.color = '#86efac';

            // ── Save frame to disk via hidden <a> download (client-side) ─────
            // NOTE: actual server-side saving is handled in main.py (see below).
            // This client-side download is optional/extra; remove if not wanted.
            // saveFrameLocally(data.image, frameIndex);
        };
        img.src = data.image;
    });

    // ── Socket lifecycle ───────────────────────────────────────────────────────
    socket.on('connect', function () {
        console.log('WebSocket connected');
        statusBadge.style.color = '#facc15';
        statusBadge.textContent = '🟡 Connected';
    });

    socket.on('disconnect', function () {
        console.log('WebSocket disconnected');
        awaitingReply = false;
        statusBadge.style.color = '#f87171';
        statusBadge.textContent = '🔴 Disconnected';
        overlayCtx.clearRect(0, 0, overlay.width, overlay.height);
        infoPanel.textContent = 'Disconnected from server.';
    });

    socket.on('error', function (err) {
        console.error('Server error:', err);
        awaitingReply = false;
        statusBadge.style.color = '#f87171';
        statusBadge.textContent = '⚠️ Server error';
    });

    socket.on('status', function (data) {
        console.log('Model status:', data);
        if (!data.model_loaded) {
            infoPanel.textContent = '⚠️ Model not loaded on server yet.';
        }
    });

    // ── Optional: save frame as download in browser ────────────────────────────
    // function saveFrameLocally(dataUrl, index) {
    //     var a = document.createElement('a');
    //     a.href = dataUrl;
    //     a.download = `frame_${String(index).padStart(5, '0')}.jpg`;
    //     a.click();
    // }
});
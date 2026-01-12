import runpod
from runpod.serverless.utils import rp_upload
import os
import websocket
import base64
import json
import uuid
import logging
import urllib.request
import urllib.parse
import binascii # Base64 에러 처리를 위해 import
import subprocess
import time
# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


server_address = os.getenv('SERVER_ADDRESS', '127.0.0.1')
client_id = str(uuid.uuid4())
def to_nearest_multiple_of_16(value):
    """주어진 값을 가장 가까운 16의 배수로 보정, 최소 16 보장"""
    try:
        numeric_value = float(value)
    except Exception:
        raise Exception(f"width/height 값이 숫자가 아닙니다: {value}")
    adjusted = int(round(numeric_value / 16.0) * 16)
    if adjusted < 16:
        adjusted = 16
    return adjusted
def process_input(input_data, temp_dir, output_filename, input_type):
    """입력 데이터를 처리하여 파일 경로를 반환하는 함수"""
    if input_type == "path":
        # 경로인 경우 그대로 반환
        logger.info(f"📁 경로 입력 처리: {input_data}")
        return input_data
    elif input_type == "url":
        # URL인 경우 다운로드
        logger.info(f"🌐 URL 입력 처리: {input_data}")
        os.makedirs(temp_dir, exist_ok=True)
        file_path = os.path.abspath(os.path.join(temp_dir, output_filename))
        return download_file_from_url(input_data, file_path)
    elif input_type == "base64":
        # Base64인 경우 디코딩하여 저장
        logger.info(f"🔢 Base64 입력 처리")
        return save_base64_to_file(input_data, temp_dir, output_filename)
    else:
        raise Exception(f"지원하지 않는 입력 타입: {input_type}")

        
def download_file_from_url(url, output_path):
    """URL에서 파일을 다운로드하는 함수"""
    try:
        # wget을 사용하여 파일 다운로드
        result = subprocess.run([
            'wget', '-O', output_path, '--no-verbose', url
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"✅ URL에서 파일을 성공적으로 다운로드했습니다: {url} -> {output_path}")
            return output_path
        else:
            logger.error(f"❌ wget 다운로드 실패: {result.stderr}")
            raise Exception(f"URL 다운로드 실패: {result.stderr}")
    except subprocess.TimeoutExpired:
        logger.error("❌ 다운로드 시간 초과")
        raise Exception("다운로드 시간 초과")
    except Exception as e:
        logger.error(f"❌ 다운로드 중 오류 발생: {e}")
        raise Exception(f"다운로드 중 오류 발생: {e}")


def save_base64_to_file(base64_data, temp_dir, output_filename):
    """Base64 데이터를 파일로 저장하는 함수"""
    try:
        # Base64 문자열 디코딩
        decoded_data = base64.b64decode(base64_data)
        
        # 디렉토리가 존재하지 않으면 생성
        os.makedirs(temp_dir, exist_ok=True)
        
        # 파일로 저장
        file_path = os.path.abspath(os.path.join(temp_dir, output_filename))
        with open(file_path, 'wb') as f:
            f.write(decoded_data)
        
        logger.info(f"✅ Base64 입력을 '{file_path}' 파일로 저장했습니다.")
        return file_path
    except (binascii.Error, ValueError) as e:
        logger.error(f"❌ Base64 디코딩 실패: {e}")
        raise Exception(f"Base64 디코딩 실패: {e}")
    
def queue_prompt(prompt):
    url = f"http://{server_address}:8188/prompt"
    logger.info(f"Queueing prompt to: {url}")
    p = {"prompt": prompt, "client_id": client_id}
    data = json.dumps(p).encode('utf-8')
    req = urllib.request.Request(url, data=data)
    return json.loads(urllib.request.urlopen(req).read())

def get_image(filename, subfolder, folder_type):
    url = f"http://{server_address}:8188/view"
    logger.info(f"Getting image from: {url}")
    data = {"filename": filename, "subfolder": subfolder, "type": folder_type}
    url_values = urllib.parse.urlencode(data)
    with urllib.request.urlopen(f"{url}?{url_values}") as response:
        return response.read()

def get_history(prompt_id):
    url = f"http://{server_address}:8188/history/{prompt_id}"
    logger.info(f"Getting history from: {url}")
    with urllib.request.urlopen(url) as response:
        return json.loads(response.read())

def get_videos(ws, prompt):
    prompt_id = queue_prompt(prompt)['prompt_id']
    output_videos = {}
    while True:
        out = ws.recv()
        if isinstance(out, str):
            message = json.loads(out)
            if message['type'] == 'executing':
                data = message['data']
                if data['node'] is None and data['prompt_id'] == prompt_id:
                    break
        else:
            continue

    history = get_history(prompt_id)[prompt_id]
    for node_id in history['outputs']:
        node_output = history['outputs'][node_id]
        videos_output = []
        # 'gifs' 또는 'videos' 키 확인
        video_list = None
        if 'gifs' in node_output:
            video_list = node_output['gifs']
        elif 'videos' in node_output:
            video_list = node_output['videos']
        
        if video_list:
            for video in video_list:
                # fullpath를 이용하여 직접 파일을 읽고 base64로 인코딩
                if 'fullpath' in video:
                    with open(video['fullpath'], 'rb') as f:
                        video_data = base64.b64encode(f.read()).decode('utf-8')
                    videos_output.append(video_data)
        output_videos[node_id] = videos_output

    return output_videos

def load_workflow(workflow_path):
    # 현재 파일의 디렉토리를 기준으로 절대 경로 생성
    current_dir = os.path.dirname(os.path.abspath(__file__))
    absolute_path = os.path.join(current_dir, workflow_path)
    with open(absolute_path, 'r') as file:
        return json.load(file)

def handler(job):
    job_input = job.get("input", {})

    logger.info(f"Received job input: {job_input}")
    task_id = f"task_{uuid.uuid4()}"

    # 이미지 입력 확인 (image_path, image_url, image_base64 중 하나라도 있으면 I2V)
    image_path = None
    has_image = False
    
    if "image_path" in job_input:
        image_path = process_input(job_input["image_path"], task_id, "input_image.jpg", "path")
        has_image = True
    elif "image_url" in job_input:
        image_path = process_input(job_input["image_url"], task_id, "input_image.jpg", "url")
        has_image = True
    elif "image_base64" in job_input:
        image_path = process_input(job_input["image_base64"], task_id, "input_image.jpg", "base64")
        has_image = True
    
    # 워크플로우 파일 선택 (이미지가 있으면 I2V, 없으면 T2V)
    if has_image:
        workflow_file = "workflow/video_ltx2_i2v.json"
        workflow_type = "I2V"
        logger.info("이미지 입력이 감지되어 I2V (Image-to-Video) 워크플로우를 사용합니다.")
    else:
        workflow_file = "workflow/video_ltx2_t2v.json"
        workflow_type = "T2V"
        logger.info("이미지 입력이 없어 T2V (Text-to-Video) 워크플로우를 사용합니다.")
    
    logger.info(f"Using {workflow_type} workflow: {workflow_file}")
    
    prompt = load_workflow(workflow_file)
    
    # 프롬프트 필수 확인
    if "prompt" not in job_input or not job_input["prompt"]:
        raise Exception("프롬프트(prompt)는 필수 입력입니다.")
    
    # 기본 파라미터 설정 (워크플로우 기본값과 일치)
    length = job_input.get("length", 121)  # 92:62 기본값: 121
    steps = job_input.get("steps", 20)  # 92:9 기본값: 20
    seed = job_input.get("seed", 10)  # 92:11 기본값: 10
    cfg = job_input.get("cfg", 4.0)  # 92:47 기본값: 4
    width = job_input.get("width", 1280)  # 92:89 기본값: 1280
    height = job_input.get("height", 720)  # 92:89 기본값: 720
    # frame_rate는 워크플로우에 따라 다름: T2V는 24, I2V는 25
    frame_rate = job_input.get("frame_rate", 25.0 if has_image else 24.0)
    positive_prompt = job_input["prompt"]
    negative_prompt = job_input.get("negative_prompt", "blurry, low quality, still frame, frames, watermark, overlay, titles, has blurbox, has subtitles")
    
    # 해상도 16배수 보정
    adjusted_width = to_nearest_multiple_of_16(width)
    adjusted_height = to_nearest_multiple_of_16(height)
    if adjusted_width != width:
        logger.info(f"Width adjusted to nearest multiple of 16: {width} -> {adjusted_width}")
    if adjusted_height != height:
        logger.info(f"Height adjusted to nearest multiple of 16: {height} -> {adjusted_height}")
    
    # 공통 노드 설정
    # 프롬프트 설정 (92:3 - positive, 92:4 - negative)
    prompt["92:3"]["inputs"]["text"] = positive_prompt
    prompt["92:4"]["inputs"]["text"] = negative_prompt
    
    # Length 설정 (92:62)
    prompt["92:62"]["inputs"]["value"] = length
    
    # Seed 설정 (92:11)
    prompt["92:11"]["inputs"]["noise_seed"] = seed
    
    # Steps 설정 (92:9 - LTXVScheduler)
    prompt["92:9"]["inputs"]["steps"] = steps
    
    # CFG 설정 (92:47 - CFGGuider)
    prompt["92:47"]["inputs"]["cfg"] = cfg
    
    # I2V 전용 설정
    if has_image:
        # 이미지 로드 (98)
        prompt["98"]["inputs"]["image"] = image_path
        
        # 이미지 리사이즈 설정 (102)
        prompt["102"]["inputs"]["resize_type.width"] = adjusted_width
        prompt["102"]["inputs"]["resize_type.height"] = adjusted_height
        
        # I2V 워크플로우의 frame_rate 설정 (92:51, 92:22, 92:97)
        # 92:99는 LTXVPreprocess 노드이므로 frame_rate 설정 안 함
        if "92:51" in prompt:
            prompt["92:51"]["inputs"]["frame_rate"] = int(frame_rate)
        if "92:22" in prompt:
            prompt["92:22"]["inputs"]["frame_rate"] = int(frame_rate)
        if "92:97" in prompt and "fps" in prompt["92:97"]["inputs"]:
            prompt["92:97"]["inputs"]["fps"] = int(frame_rate)
    else:
        # T2V 전용 설정
        # EmptyImage 설정 (92:89)
        prompt["92:89"]["inputs"]["width"] = adjusted_width
        prompt["92:89"]["inputs"]["height"] = adjusted_height
        
        # T2V 워크플로우의 frame_rate 설정
        # 92:102 (float)와 92:99 (int)만 설정하면 됨
        # 92:51, 92:22, 92:97은 노드 연결로 자동 설정됨
        if "92:102" in prompt:
            prompt["92:102"]["inputs"]["value"] = frame_rate
        if "92:99" in prompt and prompt["92:99"]["class_type"] == "PrimitiveInt":
            prompt["92:99"]["inputs"]["value"] = int(frame_rate)

    ws_url = f"ws://{server_address}:8188/ws?clientId={client_id}"
    logger.info(f"Connecting to WebSocket: {ws_url}")
    
    # 먼저 HTTP 연결이 가능한지 확인
    http_url = f"http://{server_address}:8188/"
    logger.info(f"Checking HTTP connection to: {http_url}")
    
    # HTTP 연결 확인 (최대 1분)
    max_http_attempts = 180
    for http_attempt in range(max_http_attempts):
        try:
            import urllib.request
            response = urllib.request.urlopen(http_url, timeout=5)
            logger.info(f"HTTP 연결 성공 (시도 {http_attempt+1})")
            break
        except Exception as e:
            logger.warning(f"HTTP 연결 실패 (시도 {http_attempt+1}/{max_http_attempts}): {e}")
            if http_attempt == max_http_attempts - 1:
                raise Exception("ComfyUI 서버에 연결할 수 없습니다. 서버가 실행 중인지 확인하세요.")
            time.sleep(1)
    
    ws = websocket.WebSocket()
    # 웹소켓 연결 시도 (최대 3분)
    max_attempts = int(180/5)  # 3분 (1초에 한 번씩 시도)
    for attempt in range(max_attempts):
        import time
        try:
            ws.connect(ws_url)
            logger.info(f"웹소켓 연결 성공 (시도 {attempt+1})")
            break
        except Exception as e:
            logger.warning(f"웹소켓 연결 실패 (시도 {attempt+1}/{max_attempts}): {e}")
            if attempt == max_attempts - 1:
                raise Exception("웹소켓 연결 시간 초과 (3분)")
            time.sleep(5)
    videos = get_videos(ws, prompt)
    ws.close()

    # 이미지가 없는 경우 처리
    for node_id in videos:
        if videos[node_id]:
            return {"video": videos[node_id][0]}
    
    return {"error": "비디오를를 찾을 수 없습니다."}

runpod.serverless.start({"handler": handler})
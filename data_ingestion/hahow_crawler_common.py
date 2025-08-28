from datetime import datetime


def flatten_tag_names(tags: list):
    return ','.join([tag.get('name','').replace('更多', '') for tag in tags])


def to_datetime(timestamp_str: str):
    """
    將 '2021-04-27T04:00:20.656Z' 這種字串轉成 2021-04-27 04:00:20 datetime 格式
    """
    if not timestamp_str:
        return None
    # 去掉 Z，T 換成空格
    timestamp_str = timestamp_str.replace('T', ' ').replace('Z', '')
    # 去掉毫秒
    if '.' in timestamp_str:
        timestamp_str = timestamp_str.split('.')[0]
    try:
        return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
    except Exception:
        return None

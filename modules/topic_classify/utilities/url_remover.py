def url_remover(text: str):
    """
    Remove url from raw content
    """
    import re

    removed_text = re.sub(r"http\S+", "", text);
    
    return removed_text;
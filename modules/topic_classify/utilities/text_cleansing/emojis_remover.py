def emojis_remover(text: str):
    """
    Remove emojis from raw content
    """
    import re

    removed_text = re.sub(r"(\u00a9|\u00ae|[\u2000-\u3300]|\ud83c[\ud000-\udfff]|\ud83d[\ud000-\udfff]|\ud83e[\ud000-\udfff])", "", text);

    return removed_text;
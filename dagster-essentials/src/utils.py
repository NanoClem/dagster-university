from datetime import datetime


def change_date_format(date_str: str, in_format: str, out_format: str) -> str:
    return datetime.strptime(date_str, in_format).strftime(out_format)

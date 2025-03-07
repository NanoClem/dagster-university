import base64
from datetime import datetime
from io import BytesIO

from matplotlib.figure import Figure


def change_date_format(date_str: str, in_format: str, out_format: str) -> str:
    return datetime.strptime(date_str, in_format).strftime(out_format)


def fig_to_markdown(fig: Figure) -> str:
    buffer = BytesIO()
    fig.savefig(buffer, format="png")
    img_data = base64.b64encode(buffer.getvalue())

    return f"![img](data:image/png;base64,{img_data.decode()})"

import datetime
import logging
import pandas as pd
import slack
from typing import List, Tuple
import ujson

SLACKCREDS_JSON = 'slackcreds.json'
LOGGER = logging.getLogger(__name__)


def client():
    with open(SLACKCREDS_JSON, 'r') as f:
        token = ujson.load(f)['BOT_TOKEN']
    return slack.WebClient(token=token)


def make_slack_block(prefix, sgt, df):
    for i in range(0, len(df), 20):
        slc = df.iloc[i:i + 20]
        text = f'*{prefix}* @ {sgt.strftime("%Y-%m-%d %H:%M:%S SGT")}\n ```{slc.to_string()}```'
        block = {
            'type': 'section',
            'text': {
                "type": "mrkdwn",
                "text": text,
            }
        }
        yield block


def send_alert(channel, alert_list_df: List[Tuple[str, pd.DataFrame]]):
    for fld, df in alert_list_df:
        now = datetime.datetime.now()
        for block in make_slack_block(f'{fld.upper()}', now, df):
            resp = client().chat_postMessage(channel=f'{channel}',
                                             blocks=[block])
            LOGGER.info(
                f'sent to slack {fld} break size[{len(df)}] channel[{channel}] resp[{resp}]'
            )


if __name__ == "__main__":
    # Test message to ensure that slack notifications are set up properly
    client = client()
    client.chat_postMessage(channel="C01UACFTMTK",
                            text="Hello from your app! :tada:")

import slack
import ujson

SLACKCREDS_JSON = 'slackcreds.json'


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


if __name__ == "__main__":
    # Test message to ensure that slack notifications are set up properly
    client = client()
    client.chat_postMessage(channel="C01UACFTMTK",
                            text="Hello from your app! :tada:")

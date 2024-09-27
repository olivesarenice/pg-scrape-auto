import json
import urllib3
import datetime


def send_telegram(job, partition, status, e):

    BOT_USERNAME = "oliverqsw_aws_sns_bot"
    BOT_TOKEN = "7203343848:AAG5bvB4cSLiIsTLJTZNMOb98A5KHCB0jMc"
    BOT_CHATIDS = ["200254604"]
    dt = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")
    message = f"{status} | {job} @ {partition}: \n\n`{e}` \n\n[{dt}]"
    responses = {}
    for chat in BOT_CHATIDS:
        send_text = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage?chat_id={chat}&parse_mode=Markdown&text={message}"
        http = urllib3.PoolManager()
        response = http.request("GET", send_text)
        responses[chat] = response.status
    return responses


if __name__ == "__main__":
    send_telegram("Hello world")

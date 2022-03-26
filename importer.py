import json
import discord
import requests


# read configs
try:
    with open('configs.json', 'r') as configs_file:
        configs = json.loads(configs_file.read())
except OSError:
    print("ERROR: could not read configs.json")
    exit(1)

discord_token = configs.get('discord_token')
if discord_token is None:
    print("ERROR: No bot token found in configs.json")
    exit(1)

elastic_token = configs.get('elastic_token')
if elastic_token is None:
    print("ERROR: No Elastic token found in configs.json")
    exit(1)

admins = configs.get('admins')
if admins is None:
    print("WARNING: No admins specified in configs.json")

elastic_base_url = configs.get('host')
if elastic_base_url is None:
    print("ERROR: No host found in configs.json")

client = discord.Client()


@client.event
async def on_ready():
    print("Logged in as {user}".format(user=client.user))


@client.event
async def on_message(message):
    if message.author == client.user:
        return
    
    payload = {
        '@timestamp': message.created_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'author': str(message.author),
        'body': message.content,
        'channel': {
            'id': message.channel.id,
            'name': message.channel.name
        },
        'attachments': [{'type': attachment.content_type, 'url': attachment.url } for attachment in message.attachments]
    }

    resp = requests.post(url=elastic_base_url + 'hideout/_doc', data=json.dumps(payload), headers={'Content-Type': 'application/json','Authorization': "ApiKey {}".format(elastic_token)}, verify=False)

    await message.channel.send("{}: {}".format(resp.status_code, resp.content))

client.run(discord_token)
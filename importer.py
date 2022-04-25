# Author: Will Anderson (@will-a)

import json
import discord
import requests
from pathlib import Path
requests.packages.urllib3.disable_warnings()


root = str(Path(__file__).parent)

# read configs
try:
    with open(f'{root}/configs.json', 'r') as configs_file:
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

superusers = configs.get('superusers')
if superusers is None:
    print("WARNING: No superusers specified in configs.json")

elastic_base_url = configs.get('host')
if elastic_base_url is None:
    print("ERROR: No host found in configs.json")
    exit(1)

elastic_index = configs.get('index')
if elastic_index is None:
    print("ERROR: Elastic index not provided")
    exit(1)

verbose = False
client = discord.Client()


async def post_message_to_elastic(message:discord.Message, verbose:bool=False) -> requests.Response:
    payload = {
        '@timestamp': message.created_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'author': {
            'full_user': str(message.author),
            'name': message.author.name,
            'id': str(message.author.id)
        },
        'body': message.content,
        'channel': {
            'id': str(message.channel.id),
            'name': message.channel.name
        },
        'attachments': [{'type': attachment.content_type, 'url': attachment.url } for attachment in message.attachments]
    }

    resp = requests.post(
        url=f'{elastic_base_url}{elastic_index}/_doc',
        data=json.dumps(payload),
        headers={'Content-Type': 'application/json','Authorization': f"ApiKey {elastic_token}"},
        verify=False
    )

    if verbose:
        await message.channel.send(f"{resp.status_code}: `{resp.content}`")


@client.event
async def on_ready():
    print(f"Logged in as {client.user}")


@client.event
async def on_message(message):
    global verbose
    if message.author == client.user:
        return
    
    if message.content is not None and message.content.startswith('!') and len(message.content) > 1 and message.author.id in superusers:
        command = message.content[1:].split(' ')[0]
        
        if command == 'ingest':
            await message.channel.send(f"Ingesting messages from {message.channel.name}...")
            message_count = 0
            async for historic_message in message.channel.history(limit=None):
                if historic_message.author != client.user:
                    await post_message_to_elastic(historic_message)
                    message_count += 1

            await message.channel.send(f"Successfully ingested {message_count} messages from {message.channel.name}.")
            return
        elif command == 'verbose':
            verbose = False if verbose else True
            await message.channel.send(f"Verbose toggled to {verbose}.")
            return

    await post_message_to_elastic(message, verbose)


client.run(discord_token)

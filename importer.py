# Author: Will Anderson (@will-a)

import json
import discord
import logging
import requests
from pathlib import Path
requests.packages.urllib3.disable_warnings()

logging.basicConfig(level=logging.INFO)
root = str(Path(__file__).parent)

# read configs
try:
    with open(f'{root}/configs.json', 'r') as configs_file:
        configs = json.loads(configs_file.read())
except OSError:
    logging.error("ERROR: could not read configs.json")
    exit(1)

discord_token = configs.get('discord_token')
if discord_token is None:
    logging.error("ERROR: No bot token found in configs.json")
    exit(1)

elastic_token = configs.get('elastic_token')
if elastic_token is None:
    logging.error("ERROR: No Elastic token found in configs.json")
    exit(1)

superusers = configs.get('superusers')
if superusers is None:
    logging.warn("WARNING: No superusers specified in configs.json")

elastic_base_url = configs.get('host')
if elastic_base_url is None:
    logging.error("ERROR: No host found in configs.json")
    exit(1)

elastic_index = configs.get('index')
if elastic_index is None:
    logging.error("ERROR: Elastic index not provided")
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
    logging.debug(f"Payload: {payload}")

    resp = requests.post(
        url=f'{elastic_base_url}{elastic_index}/_doc',
        data=json.dumps(payload),
        headers={'Content-Type': 'application/json','Authorization': f"ApiKey {elastic_token}"},
        verify=False
    )

    logging.debug(f"Response code {resp.status_code}: {resp.text}")
    if verbose:
        await message.channel.send(f"Payload: `{payload}`\n{resp.status_code}: `{resp.content}`")


@client.event
async def on_ready():
    logging.info(f"Logged in as {client.user}")


@client.event
async def on_message(message):
    global verbose
    if message.author == client.user:
        logging.debug("Message sent by bot user, ignoring...")
        return
    
    if message.content is not None and message.content.startswith('!') and len(message.content) > 1 and message.author.id in superusers:
        command = message.content[1:].split(' ')[0]
        logging.info(f"Processing command {command}")
        
        if command == 'ingest':
            logging.info(f"Ingesting messages from {message.channel.name}")
            await message.channel.send(f"Ingesting messages from {message.channel.name}...")
            message_count = 0
            async for historic_message in message.channel.history(limit=None):
                if historic_message.author != client.user:
                    await post_message_to_elastic(historic_message)
                    message_count += 1

            logging.info(f"Successfully ingested {message_count} messages from {message.channel.name}.")
            await message.channel.send(f"Successfully ingested {message_count} messages from {message.channel.name}.")
            return
        elif command == 'verbose':
            verbose = False if verbose else True
            logging.info(f"Verbose toggled to {verbose}.")
            await message.channel.send(f"Verbose toggled to {verbose}.")
            return

    await post_message_to_elastic(message, verbose)


client.run(discord_token)

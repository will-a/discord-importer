import json
import discord
import requests
requests.packages.urllib3.disable_warnings()


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

superusers = configs.get('superusers')
if superusers is None:
    print("WARNING: No superusers specified in configs.json")

elastic_base_url = configs.get('host')
if elastic_base_url is None:
    print("ERROR: No host found in configs.json")

verbose = False
client = discord.Client()


async def post_message_to_elastic(message:discord.Message, verbose:bool=False) -> requests.Response:
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

    resp = requests.post(
        url=elastic_base_url + 'hideout/_doc',
        data=json.dumps(payload),
        headers={'Content-Type': 'application/json','Authorization': "ApiKey {}".format(elastic_token)},
        verify=False
    )

    if verbose:
        await message.channel.send("{}: {}".format(resp.status_code, resp.content))


@client.event
async def on_ready():
    print("Logged in as {user}".format(user=client.user))


@client.event
async def on_message(message):
    global verbose
    if message.author == client.user:
        return
    
    if message.content is not None and message.content.startswith('!') and len(message.content) > 1 and message.author.id in superusers:
        command = message.content[1:].split(' ')[0]
        
        if command == 'ingest':
            await message.channel.send("Ingesting messages from {channel}...".format(channel=message.channel.name))
            message_count = 0
            async for historic_message in message.channel.history(limit=None):
                if historic_message.author != client.user:
                    await post_message_to_elastic(historic_message)
                    message_count += 1
            
            await message.channel.send("Successfully ingested {num_messages} messages from {channel}.".format(
                num_messages=message_count, channel=message.channel.name
            ))
            return
        elif command == 'verbose':
            verbose = False if verbose else True
            await message.channel.send("Verbose toggled to {verbose}.".format(verbose=verbose))
            return

    await post_message_to_elastic(message, verbose)

client.run(discord_token)
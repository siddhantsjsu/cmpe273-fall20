import sys
import yaml
import requests
import schedule
import pprint

def main():
    if len(sys.argv) < 2:
        print("Too few arguments")
        return
    parsedFile = parseYAML()
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(parsedFile)
    step1 = parsedFile.get('Steps')[0].get(1)
    runStep(step1)
    

def parseYAML():
    stream = open(sys.argv[1])
    parsedYaml = yaml.load(stream, Loader=yaml.FullLoader)
    if (parsedYaml == None or parsedYaml.get('Steps') == None or parsedYaml.get('Scheduler') == None):
        print("Invalid YAML File")
        return
    return parsedYaml


def runStep(step):
    req = createRequest(step.get('method'),step.get('outbound_url'))
    getConditionalOutput(req,step.get('condition'))

def createRequest(method, url):
    restMethods = {
        'GET' : requests.get(url),
        'HEAD' : requests.head(url),
        'OPTIONS' : requests.options(url)
    }
    r = restMethods.get(method)
    return r

def getConditionalOutput(req,conditon):
    data = {
        'http.response.code': req.status_code,
        'http.response.headers.content-type' : req.headers.get('content-type'),
        'Error' : 'Error',
        'http.response.headers.X-Ratelimit-Limit' : req.headers.get('X-Ratelimit-Limit') 

    }
    operations = {
        'equal' : data.get(conditon.get('if').get('equal').get('left')) == conditon.get('if').get('equal').get('right')
    }
    if(operations.get(list(conditon.get('if').keys())[0])):
        doAction(conditon.get('then').get('action'),data.get(conditon.get('then').get('data')))
    
    else:
        doAction(conditon.get('else').get('action'),data.get(conditon.get('else').get('data')))

def doAction(action, data):
    if action == '::print':
        print(data)
    elif action[:13] == '::invoke:step':
        print(action[-1])

if __name__ == "__main__":
    main()
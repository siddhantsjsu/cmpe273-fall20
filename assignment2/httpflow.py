import sys
import yaml
import requests
import schedule
import pprint

class HttpFlow:

    def __init__(self):
        self.parsedFile = None
        self.numSteps = 0
        self.stepOrder = []
        self.cronTime = []

    def do(self):
        self.parseYAML()
        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint(self.parsedFile)
        if self.cronTime[0] != '*' and self.cronTime[1] == '*' and self.cronTime[2] == '*':
            schedule.every(int(self.cronTime[0])).minutes.do(self.runFlow)
        
        while True:
            schedule.run_pending()

    def parseYAML(self):
        stream = open(sys.argv[1])
        parsedYaml = yaml.load(stream, Loader=yaml.FullLoader)
        if (parsedYaml == None or parsedYaml.get('Steps') == None or parsedYaml.get('Scheduler') == None):
            print("Invalid YAML File")
            return
        self.parsedFile = parsedYaml
        self.numSteps = len(parsedYaml.get('Steps'))
        self.stepOrder = self.parsedFile.get('Scheduler').get('step_id_to_execute')
        self.cronTime = self.parsedFile.get('Scheduler').get('when').split(' ')

    def runFlow(self):
        for step in self.stepOrder:
            self.runStep(int(step))


    def runStep(self, stepNumber, data=''):
        step = self.parsedFile.get('Steps')[stepNumber - 1].get(stepNumber)
        if step.get('outbound_url') == '::input:data':
            req = self.createRequest(step.get('method'),data)
        else:
            req = self.createRequest(step.get('method'),step.get('outbound_url'))
        self.getConditionalOutput(req,step.get('condition'))

    def createRequest(self, method, url):
        restMethods = {
            'GET' : requests.get(url),
            'HEAD' : requests.head(url),
            'OPTIONS' : requests.options(url)
        }
        r = restMethods.get(method)
        return r

    def getConditionalOutput(self, req, conditon):
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
            self.doAction(conditon.get('then').get('action'),data.get(conditon.get('then').get('data')))
        else:
            self.doAction(conditon.get('else').get('action'),data.get(conditon.get('else').get('data')))

    def doAction(self, action, data):
        if action == '::print':
            print(data)
        elif action[:14] == '::invoke:step:':
            stepNumber = int(action[-1])
            if stepNumber > 0 and stepNumber <= self.numSteps:
                self.runStep(stepNumber, data)
            else:
                print('Error')
            

def main():
    if len(sys.argv) < 2:
        print("Too few arguments")
        return
    httpFlow = HttpFlow()
    httpFlow.do()

    

if __name__ == "__main__":
    main()
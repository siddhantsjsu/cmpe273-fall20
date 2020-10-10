import sys, yaml, requests, schedule, time

class HttpFlow:

    def __init__(self):
        self.parsedFile = None
        self.numSteps = 0
        self.stepOrder = []
        self.cronTime = []
        self.dayMapping = {
                '0' : schedule.every().sunday,
                '1' : schedule.every().monday,
                '2' : schedule.every().tuesday,
                '3' : schedule.every().wednesday,
                '4' : schedule.every().thursday,
                '5' : schedule.every().friday,
                '6' : schedule.every().saturday
            }

    def scheduleFlow(self): #Schedule the execution of http flow 
        self.parseYAML() # parse YAML File
        
        if self.cronTime[0] != '*' and self.cronTime[1] == '*' and self.cronTime[2] == '*': # E.g. 5 * * - Run every 5 minutes
            schedule.every(int(self.cronTime[0])).minutes.do(self.runFlow)

        elif self.cronTime[0] != '*' and self.cronTime[1] != '*' and self.cronTime[2] == '*':# E.g. 7 21 * - Run at 21:07 Every day
            schedule.every().day.at(self.cronTime[1].zfill(2)+':'+self.cronTime[0].zfill(2)).do(self.runFlow) 
        
        elif self.cronTime[0] != '*' and self.cronTime[1] != '*' and self.cronTime[2] != '*': # E.g. 7 21 2 - Run at 21:07 every Tuesday
            self.dayMapping.get(self.cronTime[2]).at(self.cronTime[1].zfill(2)+':'+self.cronTime[0].zfill(2)).do(self.runFlow)

        elif self.cronTime[0] != '*' and self.cronTime[1] == '*' and self.cronTime[2] != '*': # E.g. 7 * 2 - Run at 00:07 every Tuesday
            self.dayMapping.get(self.cronTime[2]).at('00:'+self.cronTime[0].zfill(2)).do(self.runFlow)

        elif self.cronTime[0] == '*' and self.cronTime[1] == '*' and self.cronTime[2] != '*': # E.g. * * 2 - Run at 00:00 every Tuesday
            self.dayMapping.get(self.cronTime[2]).at("00:00").do(self.runFlow)

        elif self.cronTime[0] == '*' and self.cronTime[1] != '*' and self.cronTime[2] == '*': # E.g. * 21 * - Run at 21:00 every day
            schedule.every().day.at(self.cronTime[1].zfill(2)+':00').do(self.runFlow)

        elif self.cronTime[0] == '*' and self.cronTime[1] != '*' and self.cronTime[2] != '*': # E.g. * 21 2 - Run at 21:00 every Tuesday
            self.dayMapping.get(self.cronTime[2]).at(self.cronTime[1].zfill(2)+':00').do(self.runFlow)

        elif self.cronTime[0] == '*' and self.cronTime[1] == '*' and self.cronTime[2] == '*': # Invalid schedule input
            print("Invalid Schedule")
            return
            
        print("HTTP Flow Scheduled..", schedule.jobs[0])
        print(schedule.jobs)
        while True:
            schedule.run_pending()
            time.sleep(1)

    def parseYAML(self): # Parse the YAML contents and store in dictionary
        stream = open(sys.argv[1])
        parsedYaml = yaml.load(stream, Loader=yaml.FullLoader)
        if (parsedYaml == None or parsedYaml.get('Steps') == None or parsedYaml.get('Scheduler') == None):
            print("Invalid YAML File")
            return
        self.parsedFile = parsedYaml
        self.numSteps = len(parsedYaml.get('Steps'))
        self.stepOrder = self.parsedFile.get('Scheduler').get('step_id_to_execute')
        self.cronTime = self.parsedFile.get('Scheduler').get('when').split(' ')

    def runFlow(self): # Run the steps in given order according to YAML file
        for step in self.stepOrder:
            self.runStep(int(step))

    def runStep(self, stepNumber, data=''): # Run given step number
        step = self.parsedFile.get('Steps')[stepNumber - 1].get(stepNumber)
        if step.get('outbound_url') == '::input:data':
            req = self.createRequest(step.get('method'),data)
        else:
            req = self.createRequest(step.get('method'),step.get('outbound_url'))
        self.getConditionalOutput(req,step.get('condition'))

    def createRequest(self, method, url): # Create HTTP Request as part of step
        restMethods = {
            'GET' : requests.get(url),
            'HEAD' : requests.head(url),
            'OPTIONS' : requests.options(url)
        }
        r = restMethods.get(method)
        return r

    def getConditionalOutput(self, req, conditon): # Generate Output according to YAML conditions
        reqMapping = {
            'http.response.code': req.status_code,
            'http.response.headers.content-type' : req.headers.get('content-type'),
            'http.response.headers.X-Ratelimit-Limit' : req.headers.get('X-Ratelimit-Limit') 
        }
        operations = {
            'equal' : reqMapping.get(conditon.get('if').get('equal').get('left')) == conditon.get('if').get('equal').get('right')
        }
        if(operations.get(list(conditon.get('if').keys())[0])):
            self.doAction(conditon.get('then').get('action'),conditon.get('then').get('data'), req)
        else:
            self.doAction(conditon.get('else').get('action'),conditon.get('else').get('data'), req)

    def doAction(self, action, data, req): # Perform action as specified in YAML
        reqMapping = {
            'http.response.code': req.status_code,
            'http.response.headers.content-type' : req.headers.get('content-type'),
            'http.response.headers.X-Ratelimit-Limit' : req.headers.get('X-Ratelimit-Limit') 
        }
        if action.startswith('::print'):
            if data in reqMapping:
                print(reqMapping.get(data))
            else:
                print(data)
        elif action.startswith('::invoke'):
            stepNumber = int(action[-1])
            if stepNumber > 0 and stepNumber <= self.numSteps:
                self.runStep(stepNumber, data=data)
            else:
                print('Error')
            
def main(): # Main Function which instantiates class and schedules flow
    if len(sys.argv) < 2:
        print("Too few arguments")
        return
    HttpFlow().scheduleFlow()


if __name__ == "__main__":
    main()
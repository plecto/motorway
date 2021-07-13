motorway
========

Requires python 3.7

Tests: [![Circle CI](https://circleci.com/gh/plecto/motorway.svg?style=svg)](https://circleci.com/gh/plecto/motorway)

Motorway is a real-time data pipeline, much like Apache Storm - but made in Python :-) We use it over at Plecto and we're really happy with it - but we're continously developing it. The reason why we started this project was that we wanted something similar to Storm, but without Zookeeper and the need to take the pipeline down to update the topology.

# Epic web interface

![Screenshot](https://www.dropbox.com/s/v614jtz0u1h9hrs/Screenshot%202016-07-29%2014.28.26.png?dl=1)

# Amazing Selling points!

- No need to "upload" topologies (in particular, no need to stop the old topology before launching the new one)
- Possibility to work tigthly with our python codebase
- "Cloud compatible" - should be able to run in AWS Auto Scaling Groups. No manual setup required for scaling and no external requirements such as Zookeeper that also do not run very nice in the Auto Scaling Groups.

# Extraordinary algorithm

Motorway re-implemented the same [algorithm to store message state](https://storm.incubator.apache.org/documentation/Acking-framework-implementation.html) as Apache Storm, which is brilliant. 

Unlike with Storm where you submit a topology to an existing cluster, with Motorway you simply add a new node with the new code and take down the other afterwards. If you want to be able to use Motorway in a HA environment (and you probably want to), you should consider running a dedicated "master node" which only handles discovery - in that way nodes can come and go as needed.

**New:** Now with pypy support for double speed!

# Use with Django

Can easily be integrated with django, if you define the pipeline (as seen below) in a management command. However, large pipelines might result in a high number of connections to your DB.


Word Count Example
==================

```python
class WordRamp(Ramp):
    sentences = [
        "Oak is strong and also gives shade.",
        "Cats and dogs each hate the other.",
        "The pipe began to rust while new.",
        "Open the crate but don't break the glass.",
        "Add the sum to the product of these three.",
        "Thieves who rob friends deserve jail.",
        "The ripe taste of cheese improves with age.",
        "Act on these orders with great speed.",
        "The hog crawled under the high fence.",
        "Move the vat over the hot fire.",
    ]

    def next(self):
        yield Message(uuid.uuid4().int, self.sentences[random.randint(0, len(self.sentences) -1)])
        
class SentenceSplitIntersection(Intersection):
    def process(self, message):
        for word in message.content.split(" "):
            yield Message.new(message, word, grouping_value=word)
        self.ack(message)


class WordCountIntersection(Intersection):
    def __init__(self):
        self._count = defaultdict(int)
        super(WordCountIntersection, self).__init__()

    @batch_process(wait=2, limit=500)
    def process(self, messages):
        for message in messages:
            self._count[message.content] += 1
            self.ack(message)
        print self._count

class WordCountPipeline(Pipeline):
    def definition(self):
        self.add_ramp(WordRamp, 'sentence')
        self.add_intersection(SentenceSplitIntersection, 'sentence', 'word')
        self.add_intersection(WordCountIntersection, 'word')


WordCountPipeline().run()
```

Integrations
============

Current list of integrations:

- Salesforce (consumer, batch + real-time)
- Recurly (consumer)
- Amazon SQS (consumer + producer)
- Amazon Kinesis (consumer + producer)
- SQL Servers (via SQLAlchemy)

Look in motorway/contrib/ for these addons and feel free to contribute additional ones.

Insights? No problem!
============
Motorway can be instrumented using New Relics python agent. Just run it using newrelic-admin and motorway 
will start sending metrics. You can find them in New Relic as non-web transactions.


License
=======
   Copyright 2014-2021 Plecto ApS

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

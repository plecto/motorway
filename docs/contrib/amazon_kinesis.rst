Amazon Kinesis
==============

The Kinesis ramp is by far the most advanced available. It actually mimicks the behavior of the Amazon Kinesis Client
Library but doesn't depend on Java like KCL.

The interface is very simple, just subclass :class:`motorway.contrib.amazon_kinesis.ramps.KinesisRamp` and add the
attribute "stream_name" according to the name you used for the stream in AWS.

Similarly, there is an intersection which allows you to "dump" content into a Kinesis stream. It works the exact same
way.

.. autoclass:: motorway.contrib.amazon_kinesis.ramps.KinesisRamp
   :members:

.. autoclass:: motorway.contrib.amazon_kinesis.intersections.KinesisInsertIntersection
   :members:
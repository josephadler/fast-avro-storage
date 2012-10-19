Fast Avro Storage
=================

I got frustrated with the version of AvroStorage bundled with Apache Pig (in
Piggybank), so I decided to write my own.

Why god why?
------------
The AvroStorage code is very complicated. It does a lot of unnecesary copying.
It doesn't support the latest version of Avro (so it doesn't support
Snappy compression). All of these things are bad.

What did you do differently?
----------------------------
I decided on a different approach. In Pig, Tuples are implemented as an 
Interface. I realized that I could just wrap Avro objects (GenericData objects)
into another object that implemented the Tuple interface. That helped reduce the
amount of copying required.

I used the latest version of Avro (1.7.2) as as starting point, and rewrote
AvroStorage from scratch.

I also wrote a function to load and store data in Trevni format from Pig.
Trevni is Doug Cutting's new format for column-oriented stores; it's
designed to accept Avro objects and return Avro objects.

Why didn't you contribute this to Apache?
-----------------------------------------
I will contribute this to Apache when I have worked out some bugs, done
some performance testing and tuning, and added unit tests. 

In the mean time, feel free to try this out. It's alpha quality software; I
can make no promises about it of any kind, other than that I wrote it. Hope
you find it useful!

-- Joe



# Crawly Project: External Processor

This acts as an independent process that runs the same "URL Processing" function. It's useful to separate processing logging from the main server logging, and to run multiple processing instances at the same time (for example, by compiling a jar that processes ascended first and a jar that runs descended first, so they can meet in the middle.)

This code is published so that others can see and use it as an example or starting point - it's not currently in a state where it can be run on it's own without tweaks and modifications. If you think you can improve this issue (such as by adding configurability) or otherwise help the project by submitting code, I welcome all contributions.

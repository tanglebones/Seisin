# Seisin

A client-server system built in rust (both client and server). Main thing I want to test is allocation of datum to threads that own them and moving that ownership around so operations across datums can run on a single owning thread (and avoiding one thread owning everything somehow).

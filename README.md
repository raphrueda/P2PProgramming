# P2PProgramming
Socket Programing Assignment for Networks Course (COMP3331)

Written in Python

Implementation of a local peer-to-peer protocol circular DHT. Each peer is run in an individual terminal. Peers are set up in a circular configuration, with each peer initially knowing its first successor (1 hop away) and its second successor (2 hops away). Peers periodically 'ping' their successors to check if they are live using UDP sockets. Planned departures and unexpected departures are supported and handled. Very simplified file hashing (abstracted to file names) is implemented. Quit and file request and response messages are handled with TCP sockets.

Setup script included.

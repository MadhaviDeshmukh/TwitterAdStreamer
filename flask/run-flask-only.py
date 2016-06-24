#!/usr/bin/env python

# Run just flask without hosting it on Tornado.
from app import app

app.run(host='0.0.0.0', debug = True)


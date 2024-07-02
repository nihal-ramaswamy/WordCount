#!/usr/bin/env python3

import random
words = ["Apple" , "Banana", "Orange", "Tomato", "Lemon", "Strawberry"]

MAX_WORDS = 10 #9000000

for i in range(MAX_WORDS):
    word = random.choice(words)
    print(word)

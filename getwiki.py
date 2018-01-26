import wikipedia

titles = [
    'New York City',
    'Berlin',
    'London',
    'Paris',
    'United States',
    'Germany',
    'France',
    'United Kingdom',
]

articles = []

for title in titles:
    print(wikipedia.page(title).content)
    print()

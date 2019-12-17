from names_dataset import NameDataset
import random


def getRandomName():
    namesDataSet = NameDataset()
    return random.choice(tuple(namesDataSet.first_names))


names = list(NameDataset().first_names)

relations = set()
for i in range(0, 10000000):
    if i % 10000 == 0:
        print(i)
    id1 = random.randint(0, len(names))
    id2 = random.randint(0, len(names))
    while (id1 == id2) or ((id1, id2) in relations) or ((id2, id1) in relations):
        id2 = random.randint(0, len(names))
    relations.add((id1, id2))

with open("names.txt", 'w', encoding='utf8') as file:
    for name, key in enumerate(names):
        file.writelines(f'{name},{key}\n')

with open("relations.txt", 'w', encoding='utf8') as file:
    for keys in relations:
        file.writelines(f'{keys[0]},{keys[1]}\n')


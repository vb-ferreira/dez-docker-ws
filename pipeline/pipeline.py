import sys
import pandas as pd

# o primeiro argumento é sempre o nome do script. Logo, pegamos o segundo, convertendo-o para integer.
month = int(sys.argv[1])

# simula ingestão de dados
df = pd.DataFrame({"day": [1, 2], "num_passengers": [1,2]})

# simula processamento de dados (adição de coluna)
df['month'] = month

# converte e salva os dados em parquet (output)
df.to_parquet(f'output_{month}.parquet')

print(f'hello pipeline, month={month}')

import requests
from pydantic import BaseModel, ConfigDict

# 1. Definição do Schema com os tipos corretos
class PokemonSchema(BaseModel):
    name: str  # CORRIGIDO: Nome alterado para String (str)
    type: str
    
    # Sintaxe atualizada para o Pydantic V2 (antigo orm_mode = True)
    model_config = ConfigDict(from_attributes=True)

def pegar_pokemon(id: int) -> PokemonSchema:
    response = requests.get(f"https://pokeapi.co/api/v2/pokemon/{id}")
    response.raise_for_status() # Boa prática: Garante que a API respondeu 200 OK
    
    data = response.json()
    
    # Uma forma mais limpa e Pythônica de extrair os tipos usando List Comprehension
    types_list = [type_info['type']['name'] for type_info in data['types']]
    types_string = ', '.join(types_list)
    
    # Retorna o objeto validado pelo Pydantic
    return PokemonSchema(name=data['name'], type=types_string)

# 2. Executando o Teste para o Pokémon ID 24 (Arbok)
pokemon = pegar_pokemon(24)
print("📊 Objeto Pydantic Resultante:")
print(pokemon)

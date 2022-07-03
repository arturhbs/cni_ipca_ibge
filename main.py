# Importar classe de outro arquivo
from bot import Bot

# Inicializar com a URL pedida no roteiro
url = 'https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1'
# Chamando a classe Bot
Bot(url).main()


from urllib.request import urlopen
import json
import pandas as pd


class Bot:
    def __init__(self, url):
        # Buscar os dados adivindos da URL passada no roteiro e fazendo verificação da validade da URL e retornar em caso de erro
        self.url = url
        try:
            self.response = urlopen(url)
        except Exception as e:
            print("Fail to get response from link: " + str(e))
        self.data_json = json.loads(self.response.read())

    def transform_data_dataframe(self, data):
        # tranforma cada campo em que é uma lista ou dicionario para que seja criado um parquet para cada um
        df_notas = data.pop('Notas', None)
        df_notas = pd.DataFrame(df_notas, columns=['Notas'])
        self._df_to_parquet(df_notas, 'df_notas')

        df_unidades_medida = data.pop('UnidadesDeMedida', None)
        df_unidades_medida = pd.DataFrame(df_unidades_medida)
        self._df_to_parquet(df_unidades_medida, 'df_unidades_medida')

        df_variaveis = data.pop('Variaveis', None)
        df_variaveis = pd.DataFrame(df_variaveis)
        self._df_to_parquet(df_variaveis, 'df_variaveis')

        df_classificacoes = data.pop('Classificacoes', None)
        df_classificacoes = self._df_normalize_json(df_classificacoes)
        self._df_to_parquet(df_classificacoes, 'df_classificacoes')

        df_pesquisa_aux = data.pop('Pesquisa', None)
        df_pesquisa = self._df_normalize_json(df_pesquisa_aux)
        self._df_to_parquet(df_pesquisa, 'df_pesquisa')

        df_periodos_aux = data.pop('Periodos', None)
        df_periodos = self._df_normalize_json(df_periodos_aux)
        self._df_to_parquet(df_periodos, 'df_periodos')

        df_territorios_aux = data.pop('Territorios', None)
        df_territorios = self._df_normalize_json(df_territorios_aux)
        self._df_to_parquet(df_territorios, 'df_territorios')

        df_data = pd.json_normalize(data)
        self._df_to_parquet(df_data, 'df_data')

    @staticmethod
    def _df_normalize_json(df):
        # Normaliza json semi-estruturados em tabelas
        df = pd.json_normalize(df)
        return df

    @staticmethod
    def _df_to_parquet(df, name):
        # Transforma o dataframe em um arquivo parquet e insere na pasta ./parquet_files
        df.to_parquet("./parquet_files/" + name + ".parquet")

    def main(self):
        # Metodo principal a ser chamado.
        self.transform_data_dataframe(self.data_json.copy())
        print('Finished')

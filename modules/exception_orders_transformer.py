import pandas as pd
import ast
import pytz

class ExceptionOrdersTransformer:
    """
    Caso de Uso responsável por transformar a base bruta de Exception Orders
    no formato consolidado de Produção EO.
    """
    
    def __init__(self, tz_str: str = 'America/Sao_Paulo'):
        self.timezone = pytz.timezone(tz_str)

    def _extract_reason(self, row) -> str:
        """
        Realiza o parse seguro da string de dicionário para extrair a descrição do motivo.
        """
        reason_str = str(row.get('reason', ''))
        operation = str(row.get('operation_string', ''))
        
        if reason_str in ['nan', 'None', ''] or '{' not in reason_str:
            return operation
            
        try:
            # Avaliação estrita e segura da estrutura de dados
            reason_dict = ast.literal_eval(reason_str)
            reason_desc = reason_dict.get('reason_desc', '')
            
            if reason_desc:
                return f"{operation} : {reason_desc}"
            return operation
        except (ValueError, SyntaxError):
            return operation

    def execute(self, raw_csv_path: str, output_csv_path: str) -> pd.DataFrame:
        """
        Executa a pipeline de transformação e gera o artefato de saída.
        Retorna o DataFrame transformado para upload ao Google Sheets.
        """
        # 1. Extração (Extract)
        try:
            df_raw = pd.read_csv(raw_csv_path)
        except FileNotFoundError:
            print(f"❌ Arquivo não encontrado: {raw_csv_path}")
            return None

        if df_raw.empty:
            print("⚠️ Arquivo de entrada vazio.")
            return None

        # 2. Transformação de Temporalidade
        # Converte de Unix Timestamp (UTC) para Datetime Local
        if 'operator_time' in df_raw.columns:
            df_raw['datetime_local'] = pd.to_datetime(
                df_raw['operator_time'], unit='s', utc=True
            ).dt.tz_convert(self.timezone)

            df_raw['date_ref'] = df_raw['datetime_local'].dt.strftime('%Y-%m-%d')
            df_raw['hora_completa'] = df_raw['datetime_local'].dt.strftime('%H:%M:%S')
            df_raw['Hora'] = df_raw['datetime_local'].dt.hour
        else:
             # Fallback ou erro se coluna crítica faltar
             print("⚠️ Coluna 'operator_time' não encontrada.")
             return None


        # 3. Transformação de Motivo
        df_raw['exception_reason'] = df_raw.apply(self._extract_reason, axis=1)

        # 4. Enriquecimento de Dados
        df_raw['Produção'] = 1

        # 5. Mapeamento de Colunas Existentes
        df_raw.rename(columns={
            'operator_station_name': 'station_name',
            'exception_order_status_string': 'Status'
        }, inplace=True)

        # 6. Seleção e Ordenação (Load in memory)
        colunas_necessarias = [
            'operator', 
            'station_name', 
            'exception_reason', 
            'date_ref', 
            'hora_completa', 
            'Hora', 
            'Produção', 
            'Status', 
            'shipment_id'
        ]
        
        # Garantir que todas as colunas existem
        for col in colunas_necessarias:
            if col not in df_raw.columns:
                 df_raw[col] = '' # Preenche com vazio se não existir

        df_final = df_raw[colunas_necessarias].copy()

        # 7. Renomear colunas para o formato da planilha de produção
        # As colunas de índice 4 ('hora_completa') e 8 ('shipment_id') ficarão com o cabeçalho vazio no CSV
        csv_columns = [
            'operator', 
            'station_name', 
            'exception_reason', 
            'date_ref', 
            '', 
            'Hora', 
            'Produção', 
            'Status', 
            ''
        ]

        # 8. Persistência do Arquivo (Carga)
        df_csv = df_final.copy()
        df_csv.columns = csv_columns
        df_csv.to_csv(output_csv_path, index=False)
        print(f"✅ Transformação concluída. Arquivo salvo em: {output_csv_path}")
        
        # Retorna DataFrame com nomes originais para upload ao Sheets
        return df_final

# Ponto de entrada para execução local pelo agente
if __name__ == "__main__":
    transformer = ExceptionOrdersTransformer()
    # Exemplo de uso
    # transformer.execute(
    #     raw_csv_path='Cockpit RTS - BASE | Exception Orders.csv',
    #     output_csv_path='Cockpit RTS - BASE | Produção EO.csv'
    # )

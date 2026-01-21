import re
from datetime import datetime, date
import math

# --- CONSTANTES ---
DDD_VALIDOS = {
    '11', '12', '13', '14', '15', '16', '17', '18', '19', 
    '21', '22', '24', '27', '28', 
    '31', '32', '33', '34', '35', '37', '38', 
    '41', '42', '43', '44', '45', '46', '47', '48', '49', 
    '51', '53', '54', '55', 
    '61', '62', '63', '64', '65', '66', '67', '68', '69', 
    '71', '73', '74', '75', '77', '79', 
    '81', '82', '83', '84', '85', '86', '87', '88', '89', 
    '91', '92', '93', '94', '95', '96', '97', '98', '99'
}

class ValidadorData:
    """Regras para Datas (1900-2050)"""

    @staticmethod
    def para_sql(data_str_ou_obj):
        """
        Entrada: '31/12/2025' ou objeto date
        Saída: '2025-12-31' (String ISO para SQL) ou None se inválido
        """
        if not data_str_ou_obj: return None
        
        obj = data_str_ou_obj
        if isinstance(data_str_ou_obj, str):
            try:
                # Remove espaços e tenta converter
                obj = datetime.strptime(data_str_ou_obj.strip(), '%d/%m/%Y').date()
            except ValueError:
                return None
        
        # Validação do Intervalo (1900 - 2050)
        if obj.year < 1900 or obj.year > 2050:
            return None
            
        return obj # Retorna objeto date (drivers SQL modernos preferem objeto)

    @staticmethod
    def para_tela(data_obj):
        """
        Entrada: 2025-12-31 (Date object)
        Saída: '31/12/2025'
        """
        if not data_obj or not isinstance(data_obj, (date, datetime)):
            return ""
        return data_obj.strftime('%d/%m/%Y')

    @staticmethod
    def calcular_tempo(data_nasc, tipo='anos'):
        """
        Calcula tempo decorrido até hoje.
        Tipos: 'anos', 'meses', 'dias', 'completo' (Anos, Meses e Dias)
        """
        if not data_nasc: return ""
        hoje = date.today()
        
        # Lógica básica
        anos = hoje.year - data_nasc.year - ((hoje.month, hoje.day) < (data_nasc.month, data_nasc.day))
        
        if tipo == 'anos':
            return anos
        
        elif tipo == 'meses':
            return (anos * 12) + (hoje.month - data_nasc.month)
            
        elif tipo == 'dias':
            return (hoje - data_nasc).days
            
        elif tipo == 'completo':
            # Cálculo aproximado elegante para visualização
            meses = hoje.month - data_nasc.month
            dias = hoje.day - data_nasc.day
            
            if dias < 0:
                meses -= 1
                dias += 30 # Aproximação comercial
                
            if meses < 0:
                meses += 12
            
            return f"{anos}a {meses}m {dias}d"
            
        return 0

class ValidadorDocumentos:
    """CPF, CNPJ e Genéricos com validação matemática"""

    @staticmethod
    def limpar_numero(valor):
        """Remove tudo que não é dígito"""
        if not valor: return ""
        return re.sub(r'\D', '', str(valor))

    @staticmethod
    def cpf_para_sql(cpf):
        """Limpa, valida Módulo 11 e preenche zeros à esquerda (11 dígitos)"""
        limpo = ValidadorDocumentos.limpar_numero(cpf)
        if not limpo: return None
        
        limpo = limpo.zfill(11) # Garante zeros à esquerda na pesquisa
        
        if len(limpo) != 11: return None
        if not ValidadorDocumentos._validar_mod11_cpf(limpo): return None
        
        return limpo

    @staticmethod
    def cnpj_para_sql(cnpj):
        """Limpa, valida Módulo 11 e preenche zeros à esquerda (14 dígitos)"""
        limpo = ValidadorDocumentos.limpar_numero(cnpj)
        if not limpo: return None
        
        limpo = limpo.zfill(14)
        
        if len(limpo) != 14: return None
        if not ValidadorDocumentos._validar_mod11_cnpj(limpo): return None
        
        return limpo

    @staticmethod
    def cpf_para_tela(cpf_limpo):
        """Entrada: '12345678901' -> Saída: '123.456.789-01'"""
        if not cpf_limpo or len(cpf_limpo) != 11: return cpf_limpo
        return f"{cpf_limpo[:3]}.{cpf_limpo[3:6]}.{cpf_limpo[6:9]}-{cpf_limpo[9:]}"

    @staticmethod
    def cnpj_para_tela(cnpj_limpo):
        """Entrada: '12345678000199' -> Saída: '12.345.678/0001-99'"""
        if not cnpj_limpo or len(cnpj_limpo) != 14: return cnpj_limpo
        return f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:]}"

    @staticmethod
    def preparar_ilike(valor):
        """Prepara valor para busca SQL segura (ex: '%123%')"""
        limpo = ValidadorDocumentos.limpar_numero(valor)
        if not limpo: return None
        return f"%{limpo}%"

    # --- Lógica Interna (Privada) ---
    @staticmethod
    def _validar_mod11_cpf(cpf):
        if cpf == cpf[0] * 11: return False
        soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
        r = (soma * 10) % 11
        d1 = 0 if r in [10, 11] else r
        if d1 != int(cpf[9]): return False
        soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
        r = (soma * 10) % 11
        d2 = 0 if r in [10, 11] else r
        return d2 == int(cpf[10])

    @staticmethod
    def _validar_mod11_cnpj(cnpj):
        if cnpj == cnpj[0] * 14: return False
        def calc(parcial, pesos):
            s = sum(int(d) * p for d, p in zip(parcial, pesos))
            r = s % 11
            return 0 if r < 2 else 11 - r
        
        d1 = calc(cnpj[:12], [5,4,3,2,9,8,7,6,5,4,3,2])
        if d1 != int(cnpj[12]): return False
        d2 = calc(cnpj[:13], [6,5,4,3,2,9,8,7,6,5,4,3,2])
        return d2 == int(cnpj[13])

class ValidadorContato:
    """Telefones, E-mails e Endereços"""

    @staticmethod
    def telefone_para_sql(tel):
        """Valida DDD e retorna apenas números (11 dígitos)"""
        limpo = ValidadorDocumentos.limpar_numero(tel)
        if not limpo or len(limpo) != 11:
            return None
        
        ddd = limpo[:2]
        if ddd not in DDD_VALIDOS:
            return None # DDD Inválido
            
        return limpo

    @staticmethod
    def telefone_para_tela(tel_limpo):
        """(11) 91234-5678"""
        if not tel_limpo or len(tel_limpo) != 11: return tel_limpo
        return f"({tel_limpo[:2]}) {tel_limpo[2:7]}-{tel_limpo[7:]}"

    @staticmethod
    def cep_para_tela(cep_limpo):
        """12345-678"""
        if not cep_limpo: return ""
        c = str(cep_limpo).zfill(8)
        if len(c) != 8: return c
        return f"{c[:5]}-{c[5:]}"

    @staticmethod
    def email_valido(email):
        """Verifica formato básico de e-mail"""
        if not email: return False
        # Regex padrão do mercado
        regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(regex, email) is not None

class ValidadorFinanceiro:
    """Moedas e Valores Decimais"""

    @staticmethod
    def para_sql(valor_str):
        """
        Entrada: "1.200,50", "1200.50", float(1200.5)
        Saída: float(1200.50) pronto para o banco
        """
        if valor_str is None: return 0.0
        if isinstance(valor_str, (int, float)): return float(valor_str)
        
        s = str(valor_str).strip()
        # Remove R$ e espaços
        s = s.replace('R$', '').strip()
        
        # Lógica para detectar formato BR (1.000,00) vs US (1,000.00)
        if ',' in s and '.' in s:
            # Assume formato BR se a vírgula estiver depois do último ponto
            if s.rfind(',') > s.rfind('.'):
                s = s.replace('.', '').replace(',', '.')
            else:
                s = s.replace(',', '') # Formato US
        elif ',' in s:
            s = s.replace(',', '.')
            
        try:
            return float(s)
        except ValueError:
            return None

    @staticmethod
    def para_tela(valor_float):
        """Saída: R$ 1.200,50"""
        if valor_float is None: return "R$ 0,00"
        try:
            val = float(valor_float)
            return f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except:
            return "R$ 0,00"

    @staticmethod
    def para_exportacao(valor_float):
        """Saída: 1200,50 (Decimal com vírgula para Excel)"""
        if valor_float is None: return "0,00"
        return f"{valor_float:.2f}".replace('.', ',')
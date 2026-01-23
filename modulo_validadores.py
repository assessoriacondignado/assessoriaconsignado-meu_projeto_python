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
    """Regras para Datas (Limite 1900-2050)"""

    @staticmethod
    def para_sql(data_str_ou_obj):
        if not data_str_ou_obj: return None
        
        obj = data_str_ou_obj
        if isinstance(data_str_ou_obj, str):
            try:
                obj = datetime.strptime(data_str_ou_obj.strip(), '%d/%m/%Y').date()
            except ValueError:
                return None
        
        if isinstance(obj, (date, datetime)):
            if obj.year < 1900 or obj.year > 2050:
                return None
            return obj
        return None

    @staticmethod
    def para_tela(data_obj):
        if not data_obj or not isinstance(data_obj, (date, datetime)):
            return ""
        return data_obj.strftime('%d/%m/%Y')

    @staticmethod
    def calcular_tempo(data_nasc, tipo='anos'):
        if not data_nasc: return ""
        hoje = date.today()
        if isinstance(data_nasc, datetime): data_nasc = data_nasc.date()
            
        try:
            anos = hoje.year - data_nasc.year - ((hoje.month, hoje.day) < (data_nasc.month, data_nasc.day))
        except: return "" 
        
        if tipo == 'anos': return anos
        elif tipo == 'meses': return (anos * 12) + (hoje.month - data_nasc.month)
        elif tipo == 'dias': return (hoje - data_nasc).days
        elif tipo == 'completo':
            meses = hoje.month - data_nasc.month
            dias = hoje.day - data_nasc.day
            if dias < 0:
                meses -= 1
                dias += 30
            if meses < 0:
                meses += 12
            return f"{anos}a {meses}m {dias}d"
        return 0

class ValidadorDocumentos:
    """CPF, CNPJ e Genéricos com validação matemática e suporte a BIGINT."""

    @staticmethod
    def limpar_numero(valor):
        if valor is None: return ""
        return re.sub(r'\D', '', str(valor))

    @staticmethod
    def cpf_para_sql(cpf_input):
        """Retorna String formatada com 11 dígitos (com zeros a esquerda)"""
        limpo = ValidadorDocumentos.limpar_numero(cpf_input)
        if not limpo: return None
        
        cpf_padronizado = limpo.zfill(11) 
        if len(cpf_padronizado) != 11: return None
        if not ValidadorDocumentos._validar_mod11_cpf(cpf_padronizado): return None
        return cpf_padronizado

    @staticmethod
    def cpf_para_bigint(cpf_input):
        """
        Retorna INTEIRO (BIGINT) para o banco.
        Ex: '075.929...' -> 75929...
        """
        limpo = ValidadorDocumentos.limpar_numero(cpf_input)
        if not limpo: return None
        try:
            return int(limpo)
        except ValueError:
            return None

    @staticmethod
    def nb_para_bigint(nb_input):
        """Trata Número de Benefício/Matrícula para BIGINT."""
        limpo = ValidadorDocumentos.limpar_numero(nb_input)
        if not limpo: return None
        try:
            return int(limpo)
        except ValueError:
            return None

    @staticmethod
    def cnpj_para_sql(cnpj_input):
        limpo = ValidadorDocumentos.limpar_numero(cnpj_input)
        if not limpo: return None
        cnpj_padronizado = limpo.zfill(14)
        if len(cnpj_padronizado) != 14: return None
        if not ValidadorDocumentos._validar_mod11_cnpj(cnpj_padronizado): return None
        return cnpj_padronizado

    @staticmethod
    def cpf_para_tela(valor):
        """Visualização: 000.000.000-00"""
        if valor is None: return ""
        s = str(valor)
        s = ValidadorDocumentos.limpar_numero(s)
        s = s.zfill(11)
        if len(s) != 11: return s
        return f"{s[:3]}.{s[3:6]}.{s[6:9]}-{s[9:]}"

    @staticmethod
    def cnpj_para_tela(valor):
        if valor is None: return ""
        s = str(valor)
        s = ValidadorDocumentos.limpar_numero(s)
        s = s.zfill(14)
        if len(s) != 14: return s
        return f"{s[:2]}.{s[2:5]}.{s[5:8]}/{s[8:12]}-{s[12:]}"

    @staticmethod
    def preparar_ilike(valor):
        limpo = ValidadorDocumentos.limpar_numero(valor)
        if not limpo: return None
        return f"%{limpo}%"

    @staticmethod
    def _validar_mod11_cpf(cpf):
        if cpf == cpf[0] * 11: return False
        try:
            soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
            r = (soma * 10) % 11
            d1 = 0 if r in [10, 11] else r
            if d1 != int(cpf[9]): return False
            soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
            r = (soma * 10) % 11
            d2 = 0 if r in [10, 11] else r
            return d2 == int(cpf[10])
        except: return False

    @staticmethod
    def _validar_mod11_cnpj(cnpj):
        if cnpj == cnpj[0] * 14: return False
        try:
            def calc(parcial, pesos):
                s = sum(int(d) * p for d, p in zip(parcial, pesos))
                r = s % 11
                return 0 if r < 2 else 11 - r
            d1 = calc(cnpj[:12], [5,4,3,2,9,8,7,6,5,4,3,2])
            if d1 != int(cnpj[12]): return False
            d2 = calc(cnpj[:13], [6,5,4,3,2,9,8,7,6,5,4,3,2])
            return d2 == int(cnpj[13])
        except: return False

class ValidadorContato:
    @staticmethod
    def telefone_para_sql(tel):
        limpo = ValidadorDocumentos.limpar_numero(tel)
        if not limpo or len(limpo) != 11: return None
        ddd = limpo[:2]
        if ddd not in DDD_VALIDOS: return None
        return limpo

    @staticmethod
    def telefone_para_tela(tel_limpo):
        if not tel_limpo: return ""
        s = str(tel_limpo)
        if len(s) != 11: return s
        return f"({s[:2]}) {s[2:7]}-{s[7:]}"

    @staticmethod
    def cep_para_tela(cep_limpo):
        if not cep_limpo: return ""
        c = str(cep_limpo).zfill(8)
        if len(c) != 8: return c
        return f"{c[:5]}-{c[5:]}"

    @staticmethod
    def email_valido(email):
        if not email: return False
        regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(regex, email) is not None

class ValidadorFinanceiro:
    @staticmethod
    def para_sql(valor_str):
        if valor_str is None or valor_str == "": return 0.0
        if isinstance(valor_str, (int, float)): return float(valor_str)
        s = str(valor_str).strip().replace('R$', '').strip()
        if ',' in s and '.' in s:
            if s.rfind(',') > s.rfind('.'): s = s.replace('.', '').replace(',', '.')
            else: s = s.replace(',', '')
        elif ',' in s: s = s.replace(',', '.')
        try: return float(s)
        except ValueError: return None

    @staticmethod
    def para_tela(valor_float):
        if valor_float is None: return "R$ 0,00"
        try:
            val = float(valor_float)
            return f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except: return "R$ 0,00"

    @staticmethod
    def para_exportacao(valor_float):
        if valor_float is None: return "0,00"
        try: return f"{float(valor_float):.2f}".replace('.', ',')
        except: return "0,00"
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
        """
        Entrada: '31/12/2025' ou objeto date
        Saída: Objeto date (2025-12-31) pronto para o banco ou None se inválido
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
        if isinstance(obj, (date, datetime)):
            if obj.year < 1900 or obj.year > 2050:
                return None
            return obj
            
        return None

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
        Tipos: 'anos', 'meses', 'dias', 'completo' (Xa Ym Zd)
        """
        if not data_nasc: return ""
        hoje = date.today()
        
        # Converte datetime para date se necessário
        if isinstance(data_nasc, datetime):
            data_nasc = data_nasc.date()
            
        try:
            anos = hoje.year - data_nasc.year - ((hoje.month, hoje.day) < (data_nasc.month, data_nasc.day))
        except:
            return "" # Erro se data for inválida
        
        if tipo == 'anos':
            return anos
        
        elif tipo == 'meses':
            return (anos * 12) + (hoje.month - data_nasc.month)
            
        elif tipo == 'dias':
            return (hoje - data_nasc).days
            
        elif tipo == 'completo':
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
    """
    CPF, CNPJ e Genéricos com validação matemática (Módulo 11).
    AJUSTE: Aceita input com ou sem zero à esquerda, padronizando para 11/14 dígitos.
    ATUALIZAÇÃO: Suporte a BIGINT para performance no banco.
    """

    @staticmethod
    def limpar_numero(valor):
        """Remove tudo que não é dígito e retorna STRING"""
        if valor is None: return ""
        return re.sub(r'\D', '', str(valor))

    @staticmethod
    def cpf_para_sql(cpf_input):
        """
        [LEGADO - Retorna String]
        Mantido para compatibilidade com partes do sistema que esperam VARCHAR.
        """
        limpo = ValidadorDocumentos.limpar_numero(cpf_input)
        if not limpo: return None
        
        cpf_padronizado = limpo.zfill(11) 
        if len(cpf_padronizado) != 11: return None
        if not ValidadorDocumentos._validar_mod11_cpf(cpf_padronizado): return None
        return cpf_padronizado

    @staticmethod
    def cpf_para_bigint(cpf_input):
        """
        [NOVO - Alta Performance]
        Valida e converte CPF para INTEIRO (BIGINT) para salvar nas tabelas otimizadas.
        Ex: '001.234.567-89' -> 123456789
        """
        # Usa a validação padrão primeiro
        cpf_str = ValidadorDocumentos.cpf_para_sql(cpf_input)
        if cpf_str:
            return int(cpf_str)
        return None

    @staticmethod
    def cnpj_para_sql(cnpj_input):
        """Padroniza para 14 dígitos e valida (Retorna String)"""
        limpo = ValidadorDocumentos.limpar_numero(cnpj_input)
        if not limpo: return None
        
        cnpj_padronizado = limpo.zfill(14)
        if len(cnpj_padronizado) != 14: return None
        if not ValidadorDocumentos._validar_mod11_cnpj(cnpj_padronizado): return None
        return cnpj_padronizado

    @staticmethod
    def nb_para_bigint(nb_input):
        """
        [NOVO] Trata Número de Benefício/Matrícula para BIGINT.
        Remove pontos e traços.
        """
        limpo = ValidadorDocumentos.limpar_numero(nb_input)
        if not limpo: return None
        try:
            return int(limpo)
        except ValueError:
            return None

    @staticmethod
    def cpf_para_tela(valor):
        """
        Visualização: 000.000.000-00
        Aceita tanto String quanto Inteiro (do banco).
        """
        if valor is None: return ""
        
        s = str(valor) # Converte int para str se necessário
        s = ValidadorDocumentos.limpar_numero(s) # Garante limpeza
        s = s.zfill(11) # Recupera o zero à esquerda
        
        if len(s) != 11: return s
        return f"{s[:3]}.{s[3:6]}.{s[6:9]}-{s[9:]}"

    @staticmethod
    def cnpj_para_tela(valor):
        """Visualização: 00.000.000/0000-00"""
        if valor is None: return ""
        s = str(valor)
        s = ValidadorDocumentos.limpar_numero(s)
        s = s.zfill(14)
        
        if len(s) != 14: return s
        return f"{s[:2]}.{s[2:5]}.{s[5:8]}/{s[8:12]}-{s[12:]}"

    @staticmethod
    def preparar_ilike(valor):
        """
        Prepara valor para busca SQL (ILIKE) em campos TEXTO.
        Para campos numéricos (BIGINT), usar conversão direta.
        """
        limpo = ValidadorDocumentos.limpar_numero(valor)
        if not limpo: return None
        return f"%{limpo}%"

    # --- Lógica Matemática (Privada) ---
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
        except:
            return False

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
        except:
            return False

class ValidadorContato:
    """Telefones, E-mails e Endereços"""

    @staticmethod
    def telefone_para_sql(tel):
        """Valida DDD e retorna apenas números (11 dígitos - String)"""
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
        if not tel_limpo: return ""
        s = str(tel_limpo) # Caso venha int do banco
        if len(s) != 11: return s
        return f"({s[:2]}) {s[2:7]}-{s[7:]}"

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
        if valor_str is None or valor_str == "": return 0.0
        if isinstance(valor_str, (int, float)): return float(valor_str)
        
        s = str(valor_str).strip()
        s = s.replace('R$', '').strip()
        
        # Lógica para detectar formato BR (1.000,00) vs US (1,000.00)
        if ',' in s and '.' in s:
            if s.rfind(',') > s.rfind('.'): # Formato BR: 1.000,00
                s = s.replace('.', '').replace(',', '.')
            else: # Formato US: 1,000.00
                s = s.replace(',', '')
        elif ',' in s: # Apenas vírgula (BR): 1500,00
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
        try:
            return f"{float(valor_float):.2f}".replace('.', ',')
        except:
            return "0,00"
"""
    Script que realiza a extração, carregamento e transformação dos datasets disponibilizados.
    
"""

# imports necessarios
from logging import exception
import pandas as pd

# path para extracao
landing_path = '/home/robson/repositorios/bike-manufacturing-company/bucket/landing'

def extractLoadTransform():
    """ 
    Esta funcao:
    - carrega os arquivos CSV's da landing zone;
    - cria dataframe pandas a partir desses CSV's;
    - faz correções/limpeza dos dataframes
    - salva os dataframes em arquivos .PARQUET na work zone.
    """
    try:
        person_path                 = landing_path + '/Person.Person.csv'
        product_path                = landing_path + '/Production.Product.csv'
        customer_path               = landing_path + '/Sales.Customer.csv'
        sales_order_detail_path     = landing_path + '/Sales.SalesOrderDetail.csv'
        sales_order_header_path     = landing_path + '/Sales.SalesOrderHeader.csv'
        special_offer_product_path  = landing_path + '/Sales.SpecialOfferProduct.csv'

    except Exception as e:
        print(str('Erro ao criar path de extracao dos arquivos csv', e))

    # carregando csv
    try: 
        person              = pd.read_csv(person_path, sep=';')
        product             = pd.read_csv(product_path, sep=';')
        customer            = pd.read_csv(customer_path, sep=';')
        sales_order_detail  = pd.read_csv(sales_order_detail_path, sep=';')
        sales_order_header  = pd.read_csv(sales_order_header_path, sep=';')
        special_offer       = pd.read_csv(special_offer_product_path, sep=';')

    except Exception as e:
        print(str('Erro ao carregar arquivos csv', e))

    # criando dataframe pandas

    try:
        person_df               = pd.DataFrame(person)
        product_df              = pd.DataFrame(product)
        customer_df             = pd.DataFrame(customer)
        sales_order_detail_df   = pd.DataFrame(sales_order_detail)
        sales_order_header_df   = pd.DataFrame(sales_order_header)
        special_offer_df        = pd.DataFrame(special_offer)

    except Exception as e:
        print(str('Erro ao criar dataframe Pandas', e))

    # -------------------------correcoes dataframe customer---------------------------

    try:
        # excluindo linhas com dados nulos em PersonID
        customer_df = customer_df.dropna(subset=['PersonID'])
        
        # excluindo a coluna StoreID
        customer_df = customer_df.drop('StoreID', axis=1)

        # mudanto tipo para "object"
        customer_df['PersonID']     = customer_df['PersonID'].astype(str)
        customer_df['CustomerID']   = customer_df['CustomerID'].astype(str)
        customer_df['TerritoryID']  = customer_df['TerritoryID'].astype(str)

        # mudando tipo para "datetime"
        customer_df['ModifiedDate'] = pd.to_datetime(customer_df.ModifiedDate)

    except Exception as e:
        print(str('Erro corrigindo dataframe customer', e))

    # -------------------------correcoes dataframe person---------------------------
    try:
        # excluindo colunas
        person_df = person_df.drop('Title', axis=1)
        person_df = person_df.drop('AdditionalContactInfo', axis=1)

        # inserindo 'uninformed' nos campos vazios de "MiddleName"
        person_df.MiddleName = person_df.MiddleName.fillna('[uninformed]')

        # mudando tipo das colunas para string
        person_df['BusinessEntityID']   = person_df['BusinessEntityID'].astype(str)
        person_df['NameStyle']          = person_df['NameStyle'].astype(str)
        person_df['EmailPromotion']     = person_df['EmailPromotion'].astype(str)
    
    except Exception as e:
        print(str('Erro corrigindo dataframe person', e))

    # -------------------------correcoes dataframe product---------------------------

    try:

        # excluindo colunas
        product_df = product_df.drop('DiscontinuedDate',axis=1)

        # mudando tipo das colunas para string
        product_df['ProductID']             = product_df['ProductID'].astype(str)
        product_df['ProductSubcategoryID']  = product_df['ProductSubcategoryID'].astype(str)
        product_df['ProductModelID']        = product_df['ProductModelID'].astype(str)

        # mudando tipo das colunas para datetime
        product_df['SellStartDate'] = pd.to_datetime(product_df.SellStartDate)
        product_df['SellEndDate']   = pd.to_datetime(product_df.SellEndDate)

        # inserindo valores padrao nos campos vazios
        product_df.Color                = product_df.Color.fillna('[uninformed]')
        product_df.Size                 = product_df.Size.fillna('[uninformed]')
        product_df.SizeUnitMeasureCode  = product_df.SizeUnitMeasureCode.fillna('[uninformed]')
        product_df.Color                = product_df.Color.fillna('[uninformed]')
        product_df.Weight               = product_df.Weight.fillna(0.0)
        product_df.ProductLine          = product_df.ProductLine.fillna('[uninformed]')
        product_df.Class                = product_df.Class.fillna('[uninformed]')
        product_df.Style                = product_df.Style.fillna('[uninformed]')
        product_df.ProductSubcategoryID = product_df.ProductSubcategoryID.fillna('[uninformed]')
        product_df.ProductModelID       = product_df.ProductModelID.fillna('[uninformed]')   

    except Exception as e:
        print(str('Erro ao corrigir dataframe product', e))

    # -------------------------correcoes dataframe salesOrderDetail---------------------------

    try:
        # inserindo valor para preencher campos vazios
        sales_order_detail_df.CarrierTrackingNumber = sales_order_detail_df.CarrierTrackingNumber.fillna('[uninformed]')

        # mudança de tipos de dados
        sales_order_detail_df['SalesOrderID']       = sales_order_detail_df['SalesOrderID'].astype(str)
        sales_order_detail_df['SalesOrderDetailID'] = sales_order_detail_df['SalesOrderDetailID'].astype(str)
        sales_order_detail_df['ProductID']          = sales_order_detail_df['ProductID'].astype(str)
        sales_order_detail_df['SpecialOfferID']     = sales_order_detail_df['SpecialOfferID'].astype(str)

        sales_order_detail_df['UnitPrice']          = sales_order_detail_df['UnitPrice'].apply(lambda x: float(x.split()[0].replace(',', '.')))
        sales_order_detail_df['UnitPriceDiscount']  = sales_order_detail_df['UnitPriceDiscount'].apply(lambda x: float(x.split()[0].replace(',', '.')))
        sales_order_detail_df['UnitPrice']          = sales_order_detail_df['UnitPrice'].astype(float)
        sales_order_detail_df['UnitPriceDiscount']  = sales_order_detail_df['UnitPriceDiscount'].astype(float)

        # mudança para datetime
        sales_order_detail_df['ModifiedDate'] = pd.to_datetime(sales_order_detail_df.ModifiedDate)
    
    except Exception as e:
        print(str('Erro ao corrigir dataframe salesOrDetail', e))

    # -------------------------correcoes dataframe salesOrderHeader---------------------------

    try:

        # escluindo colunas
        sales_order_header_df = sales_order_header_df.drop('Comment',axis=1)
        sales_order_header_df = sales_order_header_df.drop('CreditCardID',axis=1)
        sales_order_header_df = sales_order_header_df.drop('CreditCardApprovalCode',axis=1)

        # mudando tipo "int" para "object"
        sales_order_header_df['SalesOrderID']    = sales_order_header_df['SalesOrderID'].astype(str)
        sales_order_header_df['Status']          = sales_order_header_df['Status'].astype(str)
        sales_order_header_df['CustomerID']      = sales_order_header_df['CustomerID'].astype(str)
        sales_order_header_df['SalesPersonID']   = sales_order_header_df['SalesPersonID'].astype(str)
        sales_order_header_df['TerritoryID']     = sales_order_header_df['TerritoryID'].astype(str)
        sales_order_header_df['BillToAddressID'] = sales_order_header_df['BillToAddressID'].astype(str)
        sales_order_header_df['ShipToAddressID'] = sales_order_header_df['ShipToAddressID'].astype(str)
        sales_order_header_df['ShipMethodID']    = sales_order_header_df['ShipMethodID'].astype(str)
        sales_order_header_df['CurrencyRateID']  = sales_order_header_df['CurrencyRateID'].astype(str)

        # mudando "obj" para "float"
        sales_order_header_df['SubTotal'] = sales_order_header_df['SubTotal'].apply(lambda x: float(x.split()[0].replace(',', '.')))
        sales_order_header_df['SubTotal'] = sales_order_header_df['SubTotal'].astype(float)

        sales_order_header_df['TaxAmt'] = sales_order_header_df['TaxAmt'].apply(lambda x: float(x.split()[0].replace(',', '.')))
        sales_order_header_df['TaxAmt'] = sales_order_header_df['TaxAmt'].astype(float)

        sales_order_header_df['Freight'] = sales_order_header_df['Freight'].apply(lambda x: float(x.split()[0].replace(',', '.')))
        sales_order_header_df['Freight'] = sales_order_header_df['Freight'].astype(float)

        sales_order_header_df['TotalDue'] = sales_order_header_df['TotalDue'].apply(lambda x: float(x.split()[0].replace(',', '.')))
        sales_order_header_df['TotalDue'] = sales_order_header_df['TotalDue'].astype(float)

        # mudando "obj" para "datetime"
        sales_order_header_df['OrderDate']  = pd.to_datetime(sales_order_header_df.OrderDate)
        sales_order_header_df['DueDate']    = pd.to_datetime(sales_order_header_df.DueDate)
        sales_order_header_df['ShipDate']   = pd.to_datetime(sales_order_header_df.ShipDate)

        # inserindo valor para preencher campos vazios
        sales_order_header_df.PurchaseOrderNumber   = sales_order_header_df.PurchaseOrderNumber.fillna('[uninformed]')
        sales_order_header_df.SalesPersonID         = sales_order_header_df.SalesPersonID.fillna('[uninformed]')
        sales_order_header_df.CurrencyRateID        = sales_order_header_df.CurrencyRateID.fillna('[uninformed]')
    
    except Exception as e:
        print(str('Erro ao corrigir dataframe salesOrderHeader', e))

    # -------------------------correcoes dataframe specialOfferProduct---------------------------

    try:
        # "int" para "object"
        special_offer_df['SpecialOfferID']  = special_offer_df['SpecialOfferID'].astype(str)
        special_offer_df['ProductID']       = special_offer_df['ProductID'].astype(str)


        # "object" para "datetime"
        special_offer_df['ModifiedDate'] = pd.to_datetime(special_offer_df.ModifiedDate)
   
    except Exception as e:
        print(str('Erro ao corrigir dataframe specialOfferProduct', e))

        # --------------------------criacao dos arquivos parquet-------------------------------------

    try:
        parquet_path = '/home/robson/repositorios/bike-manufacturing-company/data-lake/work'

        person_df.to_parquet(parquet_path + '/person/person.parquet')
        product_df.to_parquet(parquet_path + '/production/product.parquet')
        customer_df.to_parquet(parquet_path + '/sales/customer.parquet')
        sales_order_detail_df.to_parquet(parquet_path + '/sales/sales_order_detail.parquet')
        sales_order_header_df.to_parquet(parquet_path + '/sales/sales_order_header.parquet')
        special_offer_df.to_parquet(parquet_path + '/sales/special_offer.parquet')
    
    except Exception as e:
        print(str('Erro ao salvar parquet', e))


def main():
    extractLoadTransform()
    
if __name__ == '__main__':
    main()

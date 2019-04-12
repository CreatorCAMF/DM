from __future__ import absolute_import
import apache_beam as beam
from apache_beam import pvalue
import datetime
import unicodedata
import logging
import re
logging.basicConfig(level=logging.INFO)

# Codigo para optener los diferentes PCollection de:
# nombre, sistema, genero, fecha de nacimiento, email, telefono
# rfc, calle, estado, municipio, cp
class GetRegisters(beam.DoFn):
    def process(self, element):
        register = element.split(',')
        yield [register[0], register[2]]
        yield pvalue.TaggedOutput('sistema', [register[0], register[1]])
        yield pvalue.TaggedOutput('genero', [register[0], register[3]])
        yield pvalue.TaggedOutput('fechaNac', [register[0], register[4]])
        yield pvalue.TaggedOutput('email', [register[0], register[5]])
        yield pvalue.TaggedOutput('telefono', [register[0], register[6]])
        yield pvalue.TaggedOutput('rfc', [register[0], register[7]])
        yield pvalue.TaggedOutput('calle', [register[0], register[8]])
        yield pvalue.TaggedOutput('estado', [register[0], register[9]])
        yield pvalue.TaggedOutput('municipio', [register[0], register[10]])
        yield pvalue.TaggedOutput('cp', [register[0], register[11]])


# Codigo para limpieza de nombre
class LimpiarNombre(beam.PTransform):
    def expand(self, pcoll):
        return (pcoll
                | 'Eliminar espacios' >> beam.ParDo(EliminarEspacios())
                | 'EliminarAcentos' >> beam.ParDo(EliminarAcentos())
                | 'Conservar LN' >> beam.ParDo(ConservarLetras())
                | 'Mayus' >> beam.ParDo(Mayus())
                | 'Separar' >> beam.ParDo(Separar_nombres()))


class Arreglizar(beam.DoFn):
    def process(self, element, table_name):
        res = []
        if table_name == 'cat_nombres':
            res.append(element['token'])
            res.append(element['data'])
            res.append(element['replace'])
            res.append(element['gen'])
        if table_name == 'cat_token_nombres':
            res.append(element['token_in'])
            res.append(element['token_out'])
            res.append(element['flag'])
        yield res


class ObtenerCatalogoNombres(beam.PTransform):
    def expand(self, pcoll):
        return (pcoll | 'Read cat_nombres ' >> beam.io.Read(
                beam.io.BigQuerySource(table='cat_nombres',
                                       dataset='DM',
                                       project='dataflow-5101052'))
                      | 'cat_nombres' >> beam.ParDo(Arreglizar(), 'cat_nombres'))


class ObtenerCatalogoTokenNombres(beam.PTransform):
    def expand(self, pcoll):
        return (pcoll | 'Read cat_nombres ' >> beam.io.Read(
                beam.io.BigQuerySource(table='cat_token_nombres',
                                       dataset='DM',
                                       project='dataflow-5101052'))
                      | 'cat_nombres' >> beam.ParDo(Arreglizar(), 'cat_token_nombres'))


class ValidarNombre(beam.PTransform):
    def expand(self, pcoll):
        return (pcoll | 'Limpiar nombre' >> LimpiarNombre(input_val)
                      | 'Remplazar Nombres' >> beam.ParDo(RemplazarNombre(),
                                            pvalue.AsList(cat_nombres))
                      | 'Tokenizar' >> beam.ParDo(TokenizarValidar(),
                                            pvalue.AsList(cat_token_nombres))
                      # | 'Escribir conjunto' >> beam.io.WriteToText(output_val)
                      )


class ConservarLetras(beam.DoFn):
    def process(self, element):
        yield [element[0], ''.join([c for c in element[1] if c.isalpha() or c == ' '])]


class EliminarAcentos(beam.DoFn):
    def process(self, element):
        str = unicodedata.normalize('NFD', unicode(element[1]))
        yield [element[0], ''.join([c for c in str if unicodedata.category(c) != 'Mn'])]


class Mayus(beam.DoFn):
    def process(self, element):
        yield [element[0], element[1].upper()]


class EliminarEspacios(beam.DoFn):
    def process(self, element):
        yield [element[0], re.sub(' +', ' ', element[1].strip())]


class Separar_nombres(beam.DoFn):
    def process(self, element):
        yield [element[0], element[1].split(' ')]


class Encoder(beam.DoFn):
    def process(self, element):
        res = element.encode(encoding='UTF-8', errors='ignore')
        yield res





class RemplazarNombre(beam.DoFn):
    def process(self, element, catalogo):
        res = []
        nombre = True
        for x in element[1]:
            for y in catalogo:
                if x == y[1]:
                    if y[2] is None:
                        res.append([x, y[0]])
                    else:
                        for z in catalogo:
                            if y[2] == z[1]:
                                res.append([y[2], z[0]])
                    nombre = False
                    break
            if nombre:
                res.append([x, '+'])
            nombre = True
        yield [element[0], res]


class TokenizarValidar(beam.DoFn):
    def process(self, element, cat_token_nombres):
        nombre = ''
        token = '|'
        flag = '2'
        for x in element[1]:
            nombre += x[0]+' '
            token += x[1]+'|'
        for y in cat_token_nombres:
            if token == y[0]:
                token = y[1]
                flag = y[2]
        yield [element[0], nombre.strip(), token, flag]


class UnirNombreGenero(beam.DoFn):
    def process(self, element, nombres):
        for y in nombres:
            if element[0] == y[0]:
                yield [element[0], element[1], y[1]]
                break

class ObtenerGenero(beam.DoFn):
    def process(self, element, cat_nombres):
        genero = []
        gen = None
        if element[1] != '':
            yield [element[0], element[1], element[2]]
        else:
            nombre = element[2].split(' ')
            for x in nombre:
                for y in cat_nombres:
                    if x == y[1]:
                        genero.append(y[3])
                        break
            if len(genero) > 0:
                gen = genero[0]
            else:
                gen = None
            for x in genero:
                if gen != x and x is not None:
                    gen = None
            yield [element[0], gen, element[2]]


class LimpiarFechas(beam.DoFn):
    def process(self, element, formato_deseado):
        fecha = ''
        for format in ['%d/%m/%Y', '%d %b %Y', '%a %d %b %Y',
                       '%A %d %b %Y', '%m/%d/%Y', '%Y%m%d',
                       '%Y%d%m', '%d-%m-%Y', '%Y-%m-%d',
                       '%d/%m/%y', '%d %b %y', '%a %d %b %y',
                        '%A %d %b %y', '%m/%d/%y', '%y%m%d',
                        '%y%d%m', '%d-%m-%y', '%y-%m-%d']:
            try:
                date = datetime.datetime.strptime(element[1], format) # turn the string into a unix timestamp
                fecha = datetime.datetime.strftime(date, formato_deseado)
                if date < datetime.datetime.now():
                    break
            except ValueError as e:
                fecha = None
                pass
        yield [element[0], fecha]


project_id = 'dataflow-5101052'
date = datetime.datetime.now().strftime("%Y%m%d")
time = datetime.datetime.now().strftime("%H%M%S")
tipo = 'dailyprices'
job_name = 'test2'+date+time
bucket = 'air-test'
input_val = 'gs://'+bucket+'/archivos_diarios/beam.csv'
output_val = 'gs://'+bucket+'/archivos_diarios/res_beam.txt'
output_val2 = 'gs://'+bucket+'/archivos_diarios/res_cat.txt'
output_val3 = 'gs://'+bucket+'/archivos_diarios/gen.txt'
query_cat_nombres = 'SELECT * FROM `dataflow-5101052.DM.cat_nombres`'

p = beam.Pipeline(runner="DataflowRunner", argv=[
            "--project", project_id,
            "--job_name", job_name,
            "--staging_location", ('gs://'+bucket+'/staging'),
            "--temp_location", ('gs://'+bucket+'/temp'),
            "--save_main_session", 'true'
        ])

cat_nombres = (p | 'Obtener catalogo de nombres' >> ObtenerCatalogoNombres())

cat_token_nombres = (p | 'Obtener catalogo de tokens ' >> ObtenerCatalogoTokenNombres())

registers = (p  | 'Read Alpha Data ' >> beam.io.ReadFromText(input_val, skip_header_lines=1)
                | 'Obtener datos' >> beam.ParDo(GetRegisters()).with_outputs('sistema',
                                                         'genero',
                                                         'fechaNac',
                                                         'email',
                                                         'telefono',
                                                         'rfc',
                                                         'calle',
                                                         'estado',
                                                         'municipio',
                                                         'cp',
                                                         main='nombres'))

nombres = registers["nombres"] | 'Validar nombre' >> ValidarNombre()

generos = (registers["genero"] | 'Unir Nombre Genero' >> beam.ParDo(UnirNombreGenero(),
                                                                            pvalue.AsList(nombres))
                                     | 'Obtener Genero' >> beam.ParDo(ObtenerGenero(),
                                                                        pvalue.AsList(cat_nombres)))

fechas_nacimiento = registers["fechaNac"] | 'Limpiar Fechas de nacimento' >> beam.ParDo(LimpiarFechas(),'%d-%m-%Y')

fechas_nacimiento |"Escribir registros" >> beam.io.WriteToText(output_val3)

# nombresgenero |"Escribir registros" >> beam.io.WriteToText(output_val3)

p.run()
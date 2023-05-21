from pyspark import SparkContext
import sys

# Función que devuelve las aristas como una tupla ordenada
def get_edges(line):
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 < n2:
         return (n1, n2)
    elif n1 > n2:
         return (n2, n1)
    else:
        pass  # n1 == n2 (se ignora para eliminar los bucles en el grafo)

# Función que abre el archivo, recoge las aristas, elimina los bucles y quita las aristas iguales
def get_rdd_distinct_edges(sc, filename):
    return sc.textFile(filename).\
        map(get_edges).\
        filter(lambda x: x is not None).\
        distinct()

# Función que devuelve si las aristas existen y si dependen de otro nodo
def lista_aristas_dep(tupla):
    aristas = []
    for i in range(len(tupla[1])):
        aristas.append(((tupla[0], tupla[1][i]), 'exists'))  # Arista existente
        for j in range(i+1, len(tupla[1])):
            if tupla[1][i] < tupla[1][j]:
                aristas.append(((tupla[1][i], tupla[1][j]), ('pending', tupla[0])))  # Arista pendiente con nodo de referencia
            else:
                aristas.append(((tupla[1][j], tupla[1][i]), ('pending', tupla[0])))  # Arista pendiente con nodo de referencia
    return aristas

# Función que filtra para quedarse con los triciclos
def filtro(tupla):
    return len(tupla[1]) >= 2 and 'exists' in tupla[1]  # Filtra las tuplas que representan triciclos

# Función que saca los triciclos y los devuelve en forma de tupla
def triciclos(h):
    l = []
    for i in h[1]:
        if i != 'exists':
            l.append((i[1], h[0][0], h[0][1]))  # Formato de tupla para representar los triciclos
    return l

# Función principal, se sirve de todas las anteriores
def principal(sc, filename):
    edges = get_rdd_distinct_edges(sc, filename)
    adj = edges.groupByKey().mapValues(list).flatMap(lista_aristas_dep).\
            groupByKey().mapValues(list).filter(filtro).flatMap(triciclos)
    print(adj.collect())

# Verifica si se pasó correctamente el nombre del archivo como argumento al programa
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        with SparkContext() as sc:
            principal(sc, sys.argv[1])

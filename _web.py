# Jorge Gant Ballesteros
# 
# V1 . 2021/03
#
global MyDict
class MyDict(dict):
        # __init__function
        def __init__(self):
            self = dict()

        # Function to add key:value
        def add(self, key, value):
            self[key] = value

def get_rel(arg, cl=cl,gpx=gpx):
	dc = json.loads(arg)
	table=dc['table'].encode('utf-8')
	campos_dc = Leex(FDC[gpx[1]],table)	
	#__Insp: get_rel(arg, cl=cl,gpx=gpx)|campos_dc
	if campos_dc==1:
		error(cl,'No existe la table "'+table+'"')
	relacionados = {}
	for ln in campos_dc[2]:	# Lista de campos
		cid,cdeno,cfmt,ccol,crel,cfc = ln[:6]	# IdCampo, Deno, Formato, NCols, FicheroRelacionado, FormCalculo
		if cfc<>'': continue	# Campo calculado. No se puede acceder a su valor desde un registro leido con "lee(cl,..) "
		if crel=='': continue
		relacionados[cid]=crel

	relacionados = json.dumps(relacionados)

	return relacionados

def seleccionar(args, gpx=gpx,cl=cl):
	data = []
	try:
		db, table, campos, prg, read_rel,force = args[:6]
		gpKok=(gpx[0],gpx[1],db)
		if "__all__"  in campos:
			campos_dc = Leex(FDC[gpKok[1]],table)
			if campos_dc==1:
				txt='Does not exist '+table
				return [{"status": False, "content": txt}]
			
			campos = ['0']

			for ln in campos_dc[2]:	# Lista de campos
				cid,cdeno,cfmt,ccol,crel,cfc = ln[:6]	# IdCampo, Deno, Formato, NCols, FicheroRelacionado, FormCalculo
				if cfc<>'': continue	# Campo calculado. No se puede acceder a su valor desde un registro leido con "lee(cl,..) "
				campos.append(str(cid))

		# Si hay más de 15 campos envío error. Selec da error de list index por lista larga de campos
		#if len(campos)> 15:
		#	return [{"status": False, "content": "15 Max number fields. You try to select "+str(len(campos))}]
		#return [{"status": False, "table": table, "campos": campos, "prg": prg, "gpKok": str(gpKok)}]	
		registros = selec(gpKok,table,campos,prg)
		if campos==['0']:
			registros = [[reg] for reg in registros]
		#except:
		#	return [{"status": False, "content": "Selection error. Check args. Max number of fields 15. You have"+ str(len(campos_dc[2]))}]
				
		nregs=len(registros)
		#if nregs > 5000:
		#	if not force:
		#		txt = "gsBase limitation regs. More than 5000 in selection.In this case you try "+str(nregs)+". If you want to force insert param force: True. Take care man."
		#		return [{"status": False, "content": txt}]

		campos[0]='ID'

		# Covertir formatos
		table=table.encode('utf-8')
		campos_dc = Leex(FDC[gpKok[1]],table)
		if campos_dc!=[]:
			campos_fmt = {l[0]:l[2] for l in campos_dc[2]}

		for registro in registros:
			#return [{"status": False, "content": campos}]
			reg= MyDict()
			for i in range(0,len(campos)):
				fmt='l'
				if campos_fmt.get(campos[i]):
					fmt=campos_fmt[campos[i]]
		
				if fmt == 'd':
					if registro[i]==None:
						registro[i]=''
					else:
						try:
							f_aux=Num_aFecha(registro[i])
						except:
							f_aux=registro[i]
						try:
							f_aux=f_aux.replace("/","-")
							_lsf=f_aux.split("-")
							f_aux=_lsf[2]+'-'+_lsf[1]+'-'+_lsf[0]
						except:
							f_aux=''
						registro[i]=f_aux.encode("UTF8")

				elif fmt in ['i','0','1','2','3','4','5']:
					registro[i]=round(Num(registro[i]),2)
				else:
					try:
						registro[i]=str(registro[i]).encode('UTF8')
					except:
						try:
							registro[i]=''
						except:
							txt = registro
							return [{"status": False, "content": txt}]

				# Ahorro de espacio. Las claves que no existan, será porque tienen valor nulo
				if registro[i] in ['',[]]:
					continue
				reg.add(campos[i], registro[i])

			data.append(reg)
			del reg

		data = json.dumps(data,skipkeys=True)
		# 10 mb
		tama=sys.getsizeof(data)
		if tama>10000000:
			txt = 'gsBase limitation. 10000000 bytes max for selection. '+str(tama)+' bytes in this case. Please filter your selection'
			return [{"status": False, "content": txt}]
		return data
	except:
		return [{"status": False, "content": str(Busca_Error(cl))}]

# recibimos table y lista de objetos registro sin id
def post(args, gpx=gpx,cl=cl):
	ids=[]
	db,table,regs=args[:3]
	gpKok=(gpx[0],gpx[1],db)

	fls={}
	fls[gpKok]=[table]
	ini_trans(cl,fls)

	new_idx=u_libre(gpKok,table)
	for reg in regs:
		new_rg=rg_vacio(gpKok[1],table)
		for campo in reg:
			campo_gs=Busca_Campo(campo,gpKok[1])
			new_rg[campo_gs]=reg[campo]

		p_actu(cl,gpKok, table, new_idx, new_rg)
		ids.append(new_idx)
		new_idx = Busca_Prox(new_idx)

	fin_trans(cl,1)

	return json.dumps(ids)

# recibimos table y lista de objetos registro con id
def put(args, gpx=gpx,cl=cl):
	ids=[]
	db, table,regs=args[:3]
	gpKok=(gpx[0],gpx[1],db)

	fls={}
	fls[gpKok]=[table]
	ini_trans(cl,fls)

	ids,fallo=[],[]
	for reg in regs:
		_id=reg["ID"]
		if _id != "":
			rg=lee(cl,gpKok,table,_id)
			if rg==1:
				fallo.append(_id)
			else:
				for campo in reg:
					if campo=="ID":
						continue
					campo_gs=Busca_Campo(campo,gpKok[1])
					rg[campo_gs]=reg[campo]

				p_actu(cl,gpKok, table, _id, rg)
				ids.append(_id)
		else:
			fallo.append(_id)

	fin_trans(cl,1)

	return json.dumps(ids)
	

# Le pasamos ids y table
def delete(args, gpx=gpx,cl=cl):
	db, table,ids = args[:3]
	gpKok=(gpx[0],gpx[1],db)
	
	fls={}
	fls[gpKok]=[table]
	ini_trans(cl,fls)
	del_ids = []
	for idx in ids:
		try:
			p_actu(cl,gpKok, table, idx, 0)
			del_ids.append(idx)
		except: continue

	fin_trans(cl,1)

	return del_ids


# obtener empresa de datos o directorio
if accion =='db-list':
	data=[]
	ejercicios = FAP[emges][apl][1]	# Lista de Ejercicios de la Aplicación
	for ejercicio in ejercicios:
		dc = {}
		id,name,anio=ejercicio[:3]
		id=id.lower()
		dc[id]=[str(name),anio]
		data.append(dc)

	data = json.dumps(data)
	envia(cl,data)

# obtiene nombre de todos los diccionarios
if accion=='table-list':
	dc = json.loads(arg)
	argumentos = dc['args']
	db=argumentos['database'].encode('utf-8')

	ok=Abre_Aplicacion(apl,self.FGAP)
	if ok[0]<>1:
		ok=Abre_Empresa(emges,apl,db)
		if ok==1: error(cl,'Imposible abrir ejercicio '+db)

	data=[]
	nm_apl = FAP[emges][apl][0].encode('utf-8')			 	# Nombre de la Aplicación
	for table in FDC[apl].keys():
		dc_table = {}
		deno_table = FDC[apl][table][0].encode('utf-8')
		dc_table[table]=deno_table
		data.append(dc_table)
	data = json.dumps(data)
	envia(cl,data)

#
#-- Devuelve:
#	La lista de campos de una table en una aplicación dados
#	Un diccionario, por table, con los listados asociados a esa table
#
if accion=='field-list':
	dc = json.loads(arg)
	argumentos = dc['args']
	db=argumentos['database'].encode('utf-8')
	table=argumentos['table'].encode('utf-8')

	ok=Abre_Aplicacion(apl,self.FGAP)
	if ok[0]<>1:
		ok=Abre_Empresa(emges,apl,db)
		if ok==1: error(cl,'Imposible abrir ejercicio '+db)

	campos_dc = Leex(FDC[apl],table)
	if campos_dc==1:
		error(cl,'No existe la table "'+table+'"')
	
	campos = []
	for ln in campos_dc[2]:	# Lista de campos
		reg= {}
		cid,cdeno,cfmt,ccol,crel,cfc = ln[:6]	# IdCampo, Deno, Formato, NCols, FicheroRelacionado, FormCalculo
		if cfc<>'': continue	# Campo calculado. No se puede acceder a su valor desde un registro leido con "lee(cl,..) "
		#reg[cid]=[cdeno.encode('utf-8'),crel,cfmt,ccol]
		reg[cid]=[cdeno.encode('utf-8'),cfmt]
		campos.append(reg)


	campos = json.dumps(campos)
	envia(cl,campos)

#
#-- Devuelve:
#	Diccionario con claves: campo con relacion : table relacion
#
if accion=='get_rel':
	relacionados = get_rel(arg)
	relacionados = json.loads(relacionados)
	for key in relacionados.keys():
		None
		#__Insp: get_rel|key,relacionados[key]
	envia(cl, relacionados)
if accion=='get':
	def trata_argumentos(arg,cl=cl):
		dc = json.loads(arg)
		db=dc.get('database','').encode('utf-8')
		table = dc.get('table','')
		ls_campos =dc.get('fields',[])
		if ls_campos==[]:
			ls_campos=['0']
		else:
			ls_campos.insert(0,'0') # añadimos id
		prg = dc.get('filters',[])
		read_rel = dc.get('read_rel',False)
		force = dc.get('force',False)

		return [db,table, ls_campos, prg, read_rel,force]

	data = []
	
	try:
		args = trata_argumentos(arg)
	except:
		txt = 'Fallo en argumentos'
		envia(cl,json.dumps([{"status": False, "content": txt,"arg": arg}]))
	else:
		try:
			data = seleccionar(args)
		except:
			txt = 'Fallo en seleccionar'
			envia(cl,json.dumps([{"status": False, "content": txt,"arg": str(args)}]))
		else:
			try:
				envia(cl,data)
			except:
				envia(cl,json.dumps(str(data)))

# llevo a seleccion el parametro get_rels y utilizo funcion get_rel para seleccionar nuestro cliente, vendedor, etc

if accion=='post':
	def trata_argumentos(arg,cl=cl):
		dc = json.loads(arg)
		db=dc.get('database','').encode('utf-8')
		table = dc.get('table','')
		regs = dc.get('regs',{})
		return [db,table, regs]
	data = []
	args = trata_argumentos(arg)
	data = post(args)
	envia(cl,data)

if accion=='put':
	def trata_argumentos(arg,cl=cl):
		dc = json.loads(arg)
		db=dc.get('database','').encode('utf-8')
		table = dc.get('table','')
		regs = dc.get('regs',{})
		return [db,table, regs]
	data = []
	args = trata_argumentos(arg)
	data = put(args)
	envia(cl,data)

#
# --- cogemos table e ids y los borramos
#
if accion=='delete':
	def trata_argumentos(arg,cl=cl):
		dc = json.loads(arg)
		db=dc.get('database','').encode('utf-8')
		table = dc.get('table','')
		ids = dc.get('ids',[])
		return [db,table, ids]

	data = []
	args = trata_argumentos(arg)
	data = delete(args)
	envia(cl,json.dumps(data))

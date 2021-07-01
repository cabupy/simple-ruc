'use strict'

/*

  Get Simple-RUC

  Description:
    1. Download files of RUCs [rucX.zip] from SET URL,
    2. Decompress files rucX.zip to rucX.txt
    3. Make files rucX.sql from each file rucX.txt
    4. Finally compress all files rucX.sql within a single file rucs.zip

  Author: Carlos Vallejos
  Date: Junio 2022
  Company: Vamyal S.A.
  Licence: MIT

  Enjoy !

 */

const { once } = require('events')

const {
  appendFileSync,
  existsSync,
  unlink,
  createWriteStream,
  createReadStream
} = require('fs')

const { createInterface } = require('readline')

const Path = require('path')
const Axios = require('axios')
const AdmZip = require('adm-zip')

const _URL = 'http://www.set.gov.py/rest/contents/download/collaboration/sites/PARAGUAY-SET/documents/informes-periodicos/ruc/'
const _RUCS = [...Array(10).keys()].map(value => `ruc${value}.zip`)
const _COMMIT = 10000

const _PATHZIP = 'files/zip'
const _PATHTXT = 'files/txt'
const _PATHSQL = 'files/sql'

async function getZipSET (file) {
  const url = `${_URL}${file}`

  try {
    const response = await Axios({
      method: 'GET',
      url: url,
      responseType: 'stream'
    })
    return Promise.resolve(response)
  } catch (error) {
    console.log(`Error al descargar el archivo ${file}. Mensaje: ${error.message}, ${error.stack}`)
    return Promise.reject(error)
  }
}

async function ctrlFileExists (pathFile) {
  if (existsSync(`${pathFile}`)) {
    console.log(`El archivo ${pathFile} existe.`)
    unlink(`${pathFile}`, (error) => {
      if (error) {
        console.log(`Error al borrar el archivo ${pathFile}. Mensaje: ${error.message}, ${error.stack}`)
        return Promise.reject(error)
      }
      console.log(`El archivo ${pathFile} ha sido borrado.`)
      return Promise.resolve()
    })
  } else {
    console.log(`Control: El archivo ${pathFile} no existe. No se hace nada.`)
    return Promise.resolve()
  }
}

async function downloadFile (file) {
  const path = Path.resolve(__dirname, _PATHZIP, file)
  const response = await getZipSET(file)
  const writer = createWriteStream(path)

  response.data.pipe(writer)

  // return a promise and resolve when download finishes
  return new Promise((resolve, reject) => {
    response.data.on('end', () => {
      console.log(`Se completo la descarga del archivo ${file}.`)
    })

    response.data.on('error', (error) => {
      console.log(`Error al descargar el archivo ${file}. Mensaje: ${error.message}, ${error.stack}`)
      reject(error)
    })

    writer.on('finish', () => {
      console.log(`Se completo la escritura del archivo ${file}`)
      resolve()
    })

    writer.on('error', (error) => {
      console.log(`Error al escribir el archivo ${file}. Mensaje: ${error.message}, ${error.stack}`)
      reject(error)
    })
  })
}

async function compressRUCS () {
  const zip = new AdmZip()

  for (const ruc of _RUCS) {
    const sqlFile = ruc.split('.')[0] + '.sql'
    zip.addLocalFile(`${_PATHSQL}/${sqlFile}`)
  }

  try {
    zip.writeZip(`${_PATHZIP}/rucs.zip`)
    console.log('Se comprimieron los archivos rucX.sql en /files/zip/rucs.zip')
    return Promise.resolve()
  } catch (error) {
    return Promise.reject(error)
  }
}

async function decompressRUC (pathFile) {
  const zip = new AdmZip(pathFile)

  const txtFile = pathFile.split('/')[pathFile.split('/').length - 1].split('.')[0] + '.txt'

  try {
    await ctrlFileExists(`${_PATHTXT}/${txtFile}`)
    zip.extractEntryTo(txtFile, Path.resolve(__dirname, _PATHTXT))
    // return Promise.resolve(`El archivo ${pathFile} ha sido descomprimido.`)
    console.log(`El archivo ${pathFile} ha sido descomprimido.`)
    return Promise.resolve(txtFile)
  } catch (error) {
    console.log(`Error al descomprimir el archivo ${pathFile}. Mensaje: ${error.message}, ${error.stack}`)
    return Promise.reject(error)
  }
}

async function toSQL (file) {
  let contentFile = ''
  const sqlFile = file.split('.')[0] + '.sql'

  try {
    const rl = createInterface({
      input: createReadStream(`${_PATHTXT}/${file}`, { encoding: 'utf8' }),
      crlfDelay: Infinity
    })

    const sqlInsert = 'INSERT INTO contribuyente (ruc, nombre, dv, anterior ) VALUES \n'
    let count = 0

    contentFile += sqlInsert

    rl.on('line', (line) => {
      count++
      const linea = line.split('|')

      const contribuyente = {
        ruc: linea[0].trim(),
        nombre: linea[1].trim().replace(/'/g, "''"),
        dv: linea[2].trim(),
        anterior: linea[3].trim().replace(/'/g, "''")
      }

      contentFile += `( '${contribuyente.ruc}', '${contribuyente.nombre}', '${contribuyente.dv}', '${contribuyente.anterior}' )${(count % _COMMIT) === 0 ? ';\n' + sqlInsert : ',\n'}`
    })

    await once(rl, 'close')
    console.log(`Lectura del archivo ${_PATHTXT}/${file} concluida. Lineas: ${count}`)
    await ctrlFileExists(`${_PATHSQL}/${sqlFile}`)
    appendFileSync(`${_PATHSQL}/${sqlFile}`, contentFile, 'utf8')
    console.log(`El archivo ${_PATHSQL}/${sqlFile} ha sido creado.`)

    return Promise.resolve()
  } catch (error) {
    console.log(`Error al crear el archivo ${_PATHSQL}/${sqlFile}. Mensaje: ${error.message}, ${error.stack}`)
    return Promise.reject(error)
  }
}

async function Main () {
  for (const ruc of _RUCS) {
    const txtFile = ruc.split('.')[0] + '.txt'
    const zipFile = ruc.split('.')[0] + '.zip'
    try {
      await downloadFile(`${ruc}`)
      const txtToSql = await decompressRUC(`${_PATHZIP}/${ruc}`)
      await toSQL(txtToSql)
      // Eliminamos los archivos txt y zip, dejamos solo .sql
      await ctrlFileExists(`${_PATHTXT}/${txtFile}`)
      await ctrlFileExists(`${_PATHZIP}/${zipFile}`)
      await compressRUCS()
    } catch (error) {
      console.log(`Error en la funcion Main(). Mensaje: ${error.message}, ${error.stack}`)
    }
  }
}

// Funcion principal de inicio
Main()

import request from 'supertest'
import { apiRoot } from '../../config'
import express from '../../services/express'
import routes, { PmiPar } from '.'

const app = () => express(apiRoot, routes)

let pmiPar

beforeEach(async () => {
  pmiPar = await PmiPar.create({})
})

test('POST /pmi-pars 201', async () => {
  const { status, body } = await request(app())
    .post(`${apiRoot}`)
  expect(status).toBe(201)
  expect(typeof body).toEqual('object')
})

test('GET /pmi-pars 200', async () => {
  const { status, body } = await request(app())
    .get(`${apiRoot}`)
  expect(status).toBe(200)
  expect(Array.isArray(body)).toBe(true)
})

test('GET /pmi-pars/:id 200', async () => {
  const { status, body } = await request(app())
    .get(`${apiRoot}/${pmiPar.id}`)
  expect(status).toBe(200)
  expect(typeof body).toEqual('object')
  expect(body.id).toEqual(pmiPar.id)
})

test('GET /pmi-pars/:id 404', async () => {
  const { status } = await request(app())
    .get(apiRoot + '/123456789098765432123456')
  expect(status).toBe(404)
})

test('PUT /pmi-pars/:id 200', async () => {
  const { status, body } = await request(app())
    .put(`${apiRoot}/${pmiPar.id}`)
  expect(status).toBe(200)
  expect(typeof body).toEqual('object')
  expect(body.id).toEqual(pmiPar.id)
})

test('PUT /pmi-pars/:id 404', async () => {
  const { status } = await request(app())
    .put(apiRoot + '/123456789098765432123456')
  expect(status).toBe(404)
})

test('DELETE /pmi-pars/:id 204', async () => {
  const { status } = await request(app())
    .delete(`${apiRoot}/${pmiPar.id}`)
  expect(status).toBe(204)
})

test('DELETE /pmi-pars/:id 404', async () => {
  const { status } = await request(app())
    .delete(apiRoot + '/123456789098765432123456')
  expect(status).toBe(404)
})

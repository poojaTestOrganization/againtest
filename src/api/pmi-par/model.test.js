import { PmiPar } from '.'

let pmiPar

beforeEach(async () => {
  pmiPar = await PmiPar.create({})
})

describe('view', () => {
  it('returns simple view', () => {
    const view = pmiPar.view()
    expect(typeof view).toBe('object')
    expect(view.id).toBe(pmiPar.id)
    expect(view.createdAt).toBeTruthy()
    expect(view.updatedAt).toBeTruthy()
  })

  it('returns full view', () => {
    const view = pmiPar.view(true)
    expect(typeof view).toBe('object')
    expect(view.id).toBe(pmiPar.id)
    expect(view.createdAt).toBeTruthy()
    expect(view.updatedAt).toBeTruthy()
  })
})

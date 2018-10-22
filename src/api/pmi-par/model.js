import mongoose, { Schema } from 'mongoose'

const pmiParSchema = new Schema({}, { timestamps: true })

pmiParSchema.methods = {
  view (full) {
    const view = {
      // simple view
      id: this.id,
      createdAt: this.createdAt,
      updatedAt: this.updatedAt
    }

    return full ? {
      ...view
      // add properties for a full view
    } : view
  }
}

const model = mongoose.model('PmiPar', pmiParSchema)

export const schema = model.schema
export default model

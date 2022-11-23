# automatically generated by the FlatBuffers compiler, do not modify

# namespace: Test

import flatbuffers

class rowObject(object):
    __slots__ = ['_tab']

    # rowObject
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # rowObject
    def RowKey(self): return self._tab.Get(flatbuffers.number_types.Int32Flags, self._tab.Pos + flatbuffers.number_types.UOffsetTFlags.py_type(0))
    # rowObject
    def Datetime(self): return self._tab.Get(flatbuffers.number_types.Int64Flags, self._tab.Pos + flatbuffers.number_types.UOffsetTFlags.py_type(8))
    # rowObject
    def SampleX(self): return self._tab.Get(flatbuffers.number_types.Float32Flags, self._tab.Pos + flatbuffers.number_types.UOffsetTFlags.py_type(16))
    # rowObject
    def SampleY(self): return self._tab.Get(flatbuffers.number_types.Float32Flags, self._tab.Pos + flatbuffers.number_types.UOffsetTFlags.py_type(20))
    # rowObject
    def SampleZ(self): return self._tab.Get(flatbuffers.number_types.Float32Flags, self._tab.Pos + flatbuffers.number_types.UOffsetTFlags.py_type(24))

def CreaterowObject(builder, rowKey, datetime, sampleX, sampleY, sampleZ):
    builder.Prep(8, 32)
    builder.Pad(4)
    builder.PrependFloat32(sampleZ)
    builder.PrependFloat32(sampleY)
    builder.PrependFloat32(sampleX)
    builder.PrependInt64(datetime)
    builder.Pad(4)
    builder.PrependInt32(rowKey)
    return builder.Offset()
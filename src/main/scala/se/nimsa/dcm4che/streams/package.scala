package se.nimsa.dcm4che

import org.dcm4che3.data.VR

import scala.language.implicitConversions

package object streams {

  implicit def toCheVR(vr: se.nimsa.dicom.data.VR.VR): VR = VR.valueOf(vr.code)

  implicit def fromCheVR(vr: VR): VR = se.nimsa.dicom.data.VR.valueOf(vr.code)
}

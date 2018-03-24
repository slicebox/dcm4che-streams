package se.nimsa.dcm4che

import org.dcm4che3.data.VR

import scala.language.implicitConversions

package object streams {

  implicit def toCheVR(vr: se.nimsa.dicom.VR.VR): VR = VR.valueOf(vr.code)

  implicit def fromCheVR(vr: VR): VR = se.nimsa.dicom.VR.valueOf(vr.code)
}

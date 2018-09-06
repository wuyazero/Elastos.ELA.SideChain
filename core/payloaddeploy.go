package core

import (
	"io"

	"github.com/elastos/Elastos.ELA.SideChain/contract"

	"github.com/elastos/Elastos.ELA.Utility/common"
)

type PayloadDeploy struct {
	Code        *contract.FunctionCode
	Params      []byte
	Name        string
	CodeVersion string
	Author      string
	Email       string
	Description string
	ProgramHash common.Uint168
}

func (dc *PayloadDeploy) Data(version byte) []byte  {
	//TODO: implement PayloadDeploy.Data()
	return []byte{0}
}

func (dc *PayloadDeploy) Serialize(w io.Writer, version byte) error {
	err := dc.Code.Serialize(w)
	if err != nil {
		return err
	}

	err = common.WriteVarBytes(w, dc.Params)
	if err != nil {
		return err
	}

	err = common.WriteVarString(w, dc.Name)
	if err != nil {
		return err
	}

	err = common.WriteVarString(w, dc.CodeVersion)
	if err != nil {
		return err
	}

	err = common.WriteVarString(w, dc.Author)
	if err != nil {
		return err
	}

	err = common.WriteVarString(w, dc.Email)
	if err != nil {
		return err
	}

	err = common.WriteVarString(w, dc.Description)
	if err != nil {
		return err
	}

	err = dc.ProgramHash.Serialize(w)
	if err != nil {
		return err
	}

	return nil
}

func (dc *PayloadDeploy) Deserialize(r io.Reader, version byte) error {
	dc.Code = new (contract.FunctionCode)
	err := dc.Code.Deserialize(r)
	if err != nil {
		return err
	}

	dc.Params, err = common.ReadVarBytes(r)
	if err != nil {
		return err
	}

	dc.Name, err = common.ReadVarString(r)
	if err != nil {
		return err
	}

	dc.CodeVersion, err = common.ReadVarString(r)
	if err != nil {
		return err
	}

	dc.Author, err = common.ReadVarString(r)
	if err != nil {
		return err
	}

	dc.Email, err = common.ReadVarString(r)
	if err != nil {
		return err
	}

	dc.Description, err = common.ReadVarString(r)
	if err != nil {
		return err
	}

	dc.ProgramHash = common.Uint168{}
	err = dc.ProgramHash.Deserialize(r)
	if err != nil {
		return err
	}

	return nil
}
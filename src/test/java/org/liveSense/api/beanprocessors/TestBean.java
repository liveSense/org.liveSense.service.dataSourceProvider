package org.liveSense.api.beanprocessors;

import java.math.BigDecimal;
import java.util.Date;

import javax.jdo.annotations.Index;
import javax.jdo.annotations.Indices;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.junit.Ignore;

@Ignore
@Entity(name="BeanTest1")
@Table(uniqueConstraints={@UniqueConstraint(columnNames={"ID_CUSTOMER"})})
@Indices(value={@Index(members={"ID_CUSTOMER"})})
public class TestBean {
	
	@Id
	@Column(name="ID", columnDefinition="INTEGER")
	private Integer id;

	@Column(name="ID_CUSTOMER", columnDefinition="INTEGER")
	@JoinTable(name="CUSTOMER", joinColumns={@JoinColumn(referencedColumnName="ID")})
	private Integer idCustomer;
	
	@Column(name="PASSWORD_ANNOTATED", columnDefinition="VARCHAR(20)")
	private String confirmationPassword;
	
	@Column(name="FOUR_PART_COLUMN_NAME", columnDefinition="INTEGER")
	private Boolean fourPartColumnName;
	
	@Column(name="BLOB_FIELD", columnDefinition="BLOB")
	private String blob;

	@Column(name="FLOAT_FIELD", columnDefinition="NUMERIC(15, 2)")
	private BigDecimal floatField;

	@Column(name="DATE_FIELD_WITH_ANNOTATION", columnDefinition="DATE")
	private Date dateFieldWithAnnotation;

	private Date dateFieldWithoutAnnotation;
	
	public Date getDateFieldWithoutAnnotation() {
		return dateFieldWithoutAnnotation;
	}
	public void setDateFieldWithoutAnnotation(Date dateFieldWithoutAnnotation) {
		this.dateFieldWithoutAnnotation = dateFieldWithoutAnnotation;
	}
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public Integer getIdCustomer() {
		return idCustomer;
	}
	public void setIdCustomer(Integer idCustomer) {
		this.idCustomer = idCustomer;
	}
	public String getConfirmationPassword() {
		return confirmationPassword;
	}
	public void setConfirmationPassword(String confirmationPassword) {
		this.confirmationPassword = confirmationPassword;
	}
	public Boolean getFourPartColumnName() {
		return fourPartColumnName;
	}
	public void setFourPartColumnName(Boolean fourPartColumnName) {
		this.fourPartColumnName = fourPartColumnName;
	}
	public String getBlob() {
		return blob;
	}
	public void setBlob(String blob) {
		this.blob = blob;
	}
	public BigDecimal getFloatField() {
		return floatField;
	}	
	public void setFloatField(BigDecimal floatField) {
		this.floatField = floatField;
	}
	public Date getDateFieldWithAnnotation() {	
		return dateFieldWithAnnotation;
	}
	public void setDateFieldWithAnnotation(
		Date dateFieldWithAnnotation) {
		this.dateFieldWithAnnotation = dateFieldWithAnnotation;
	}
}
